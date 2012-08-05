---
title: S3Logd Persistent Storage Layer
author: Matthew Sorensen
published: August 3, 2012
short: Wherein the construction of the storage layer for S3Logd is detailed.
tags: Haskell
---

Amazon's S3 is a really convenient tool for any developer looking for no-fuss 
storage for arbitrarily-large objects - in particular, it's extremely reliable and near-infinitely
scalable. However, when your credit card is on the line, "infinitely scalable" means
"arbitrarily expensive," and a number of [costly accidents](http://www.behind-the-enemy-lines.com/2012/04/google-attack-how-i-self-attacked.html)
have occurred. Amazon has [refused](https://forums.aws.amazon.com/thread.jspa?threadID=58127&tstart=75) 
to implement voluntary bandwidth caps on buckets, as they obviously have no financial incentive
to do so. Thus, I'm writing a simple little daemon that fetched my S3 logs and checks for
excessive activity or buckets that exceed a particular cost per billing period.

In order to this, one must maintain a list of total data-transfer in the current billing-period for
each buckets - reliability is important, as data-loss could result in a large charge. However,
ease of implementation and maintenance suggests that it would be nice not to pull in any dependencies 
on an external database or even something like [acid-state](http://hackage.haskell.org/package/acid-state/).
Thus, I decided to implement my own Simple Storage Service Supervisor Simple State Storage Solution.
The design is about as simple as such things get: in-memory, with an append-only journal that records
all modification events and is GC'd on startup - sorta similar to [Neil Mitchell's solution](http://neilmitchell.blogspot.com/2012/06/shake-storage-layer.html) for Shake.
I aimed merely to be decently performant, somewhat space-efficient, and pretty reliable - but the implementation
turned out much more elegantly that I thought, so it just had to be blogged.

> {-# LANGUAGE OverloadedStrings #-}
>
>module S3Logd.Monitor.Persist (
>                               Persist,
>                               incrementBandwidth,
>                               resetBandwidth,
>                               openPersist,
>                               closePersist
>                              ) where
>
>import Data.ByteString hiding (empty,take,map,foldl')
>import Data.HashMap.Strict hiding (foldl',map)
>import System.IO hiding (writeFile,readFile)
>import Control.Monad.State
>import Prelude hiding (lookup,foldl,take,all,length,writeFile,readFile)
>import Data.List (foldl')
>import System.Directory (doesFileExist,renameFile,removeFile)
>import Control.Applicative hiding (empty)
>import Blaze.ByteString.Builder.Int
>import Blaze.ByteString.Builder.ByteString
>import Blaze.ByteString.Builder.Char8 (fromChar)
>import Blaze.ByteString.Builder
>import Data.Attoparsec.ByteString
>import Data.Attoparsec.Combinator
>import Data.Monoid (mappend,mconcat)
>import Data.Bits
>import Data.Word
>import Test.QuickCheck hiding (replay)
>import System.FilePath

Events
-------

Fortunately, S3Logd's storage requirements are really very minimal: we (currently) only need to
store a natural number representing the number of bytes transferred in and out of an S3 bucket over 
a period of time. Each record thus only needs to support two possible types of mutation, namely 
being reset to a specific value and being increment by a value, which gives a simple type for
log events:

>data LogEvent = Increment ByteString Word64 | Set ByteString Word64
>                deriving(Eq,Show)

The first order of business is efficent serialization and deserialization. While the *cereal* or
*binary* packages could be used to do all of this more-or-less automatically, I've choosen to use
[*blaze-builder*](http://hackage.haskell.org/package/blaze-builder-0.3.0.2) and 
[*attoparsec*](http://hackage.haskell.org/package/attoparsec-0.10.2.0), in the theory that handling a 
possibly-incomplete record at the end of the log is much easier with attoparsec. The actual format used
isn't particularly important, so I've more-or-less abitrarility choosen *{'i' or 's' tag byte} NULL 
{bytestring} NULL {big-endian 64-bit word} NULL* . This does assume that bucket names have no null-bytes 
- this is checked elsewhere and such a restriction is prefereable to an 
[explicit length-field](http://www.cs.dartmouth.edu/~sergey/langsec/insecurity-theory-28c3.pdf).

>serialize :: LogEvent -> ByteString
>serialize (Set name size) = ser' "s\NUL" name size
>serialize (Increment name size) = ser' "i\NUL" name size
>
>ser' tag name int = toByteString $ fromByteString tag  `mappend` 
>                    fromByteString name `mappend` fromChar '\NUL' `mappend`
>                    fromWord64be int    `mappend` fromChar '\NUL'
>
>deserialize :: ByteString -> Either String [LogEvent]
>deserialize = parseOnly $ many (tag <*> name <*> size)
>    where tag = (Set <$ string "s\NUL") <|> (Increment <$ string "i\NUL")
>          name = takeWhile1 (/= 0) <* word8 0
>          size = foldl  orShiftL 0 <$> take 8 <* word8 0
>          orShiftL word64 word8 = (word64 `shiftL` 8) .|. toEnum (fromEnum word8)
>
>writeEvent :: LogEvent -> Handle -> IO ()
>writeEvent l h = hPut h $ serialize l


Testing Interlude
-------

There are a few properties of serialization/deserialization we'd like to verify: first and
foremost, *deserialize* must be the inverse of *serialize* for all *LogEvents*. Furthermore,
if we deliberately corrupt/truncate the last few bits of a serialized list of *LogEvents*, we
should still be able to parse all events except the last. In order to verify this like a "real
Haskeller" (ie. someone too lazy to write unit tests), we turn to QuickCheck.

>instance Arbitrary LogEvent where
>    arbitrary = (tag <$> arbitrary) <*> name <*> arbitrary
>        where tag True = Set
>              tag False = Increment
>              name = (pack <$> arbitrary) `suchThat` \x -> all (/= 0) x && length x > 0
>
>prop_inv :: LogEvent -> Bool
>prop_inv l = Right [l] == deserialize (serialize l)
>
>prop_corrupt :: [LogEvent] -> [Word8] -> Bool
>prop_corrupt l bs = Right l == deserialize (mconcat $ (map serialize l) ++ [pack bs])
  
Persist Datatype
-------

The actual datatype is similarly simple: it contains various references to the journal file
backing it (*path* and *handle*), a *HashMap* from bucket names to total accumulated data transfer
(*total*), a hashmap containing unlogged changes from the total data transfer (*diff*), and a
parameter (*granularity*) determining the maximum size of said differences before they are written 
out to disk. By accumulating small changes in *diff* and logging them when they exceed *granularity*,
this mechanism gives some control over the speed at which the journal grows and provides a hard 
upper limit on the amount of possible data loss per bucket in a crash.
             
>data Persist = Persist {
>      total :: HashMap ByteString Word64,
>      diff :: HashMap ByteString Word64,
>      path :: FilePath,
>      handle :: Handle,
>      granularity :: Granularity
>    } deriving (Show)
>
>type Granularity = Word64

Initialization
---------

The only semi-tricky part lies in opening a *Persist* structure from a file - the general algorithm is
as follows:

   1. If a backup of the journal exists, it it presumed that the last initialization failed, and so
   the events backup file are replayed to build the current state. Otherwise, the journal is replayed.

   2. If a backup exists, the journal is deleted. Otherwise, the journal is renamed to the backup file.

   3. The freshly-loaded state is written out as a series of *Set* events into the now-empty journal.

   4. The backup file is deleted.

First, two utility functions - *replay*, responsible for reconstructing the state from a series of events,
and *writeState*, which writes a *HashMap* as a series of events. Note that *writeState* is parameterized 
over a constructor for *LogEvent* and thus can be used to emit either *Set* or *Increment* events.

>replay :: [LogEvent] -> HashMap ByteString Word64
>replay = foldl' (flip add) empty
>    where add (Increment name inc) = adjust (+inc) name
>          add (Set name value)     = insert name value

>writeState :: Handle -> (ByteString -> Word64 -> LogEvent) -> HashMap ByteString Word64 -> IO ()
>writeState h con hash = traverseWithKey logIfNonZero hash >> return ()
>    where logIfNonZero bucket band
>              | band > 0 = writeEvent (con bucket band) h
>              | otherwise = return ()

Then the actual function responsible for loading the current state from a journal - slightly
more complicated than the previous description as it must deal with recovering from horridly corrupt
or non-existent journals.

>openPersist :: FilePath -> Granularity -> IO (Either String Persist)
>openPersist file gran = do
>  let backup = file <.> "backup"
>  fExists <- doesFileExist file
>  bExists <- doesFileExist backup
>
>  journal <- case (fExists,bExists) of
>               (_,True)      -> fmap deserialize $ readFile backup
>               (True,False)  -> fmap deserialize $ readFile file
>               (False,False) -> return $ Right []
>  case journal of
>    Left failure -> return $ Left failure
>    Right events -> do
>               case (fExists,bExists) of
>                 (True,True) -> removeFile file
>                 (False,True) -> return ()
>                 (True,False) -> renameFile file backup
>                 _ -> writeFile backup ""
>               let state = replay events
>               handle <- openFile file AppendMode
>               hSetBuffering handle NoBuffering
>               writeState handle Set state
>               removeFile backup
>               return $ Right $ Persist state empty file handle gran

Destruction
---------

Destroying a *Persist* is much simpler - all that is required is writing out all
outstanding diffs, closing the file handle, and then, if by some chance a wild backup
file has spawned, deleting it so that the current state isn't ignored.

>closePersist :: Persist -> IO ()
>closePersist p = do
>  writeState (handle p) Increment $ diff p
>  hClose $ handle p
>  let backup = path p <.> "backup"
>  exists <- doesFileExist backup
>  when exists $ removeFile backup


Modification
------

Actually incrementing the value associated with a bucket is also decently simple:
         
  + If the bucket doesn't exist in the *HashMap*s, emit a *Set* event to the journal and
    initialize the bucket's value in the *HashMap* - *total* gets the new bandwidth, and *diff*
    is initialized to 0.

  + If the bucket does exist, *granularity* comes into play - an *Increment* event is only
    emit if the *diff* plus new bandwidth is greater than *granularity*. Otherwise, the *diff* and
    *total* are just incremented by the new bandwidth.

>incrementBandwidth :: ByteString -> Word64 -> Persist -> IO (Word64 , Persist)                 
>incrementBandwidth bucket new p = maybe newBucket existingBucket $ lookup bucket $ total p
>    where newBucket = do
>              writeEvent (Set bucket new) $ handle p
>              return $! (new, updateTotalDiff p bucket new 0)           
>          existingBucket existing = do
>              let outstanding = new + lookupDefault 0 bucket (diff p)
>                  bandwidth = existing + new
>              outstanding' <- if outstanding > granularity p 
>                              then 0 <$ writeEvent (Increment bucket outstanding) (handle p)
>                              else return outstanding
>              return (bandwidth, updateTotalDiff p bucket bandwidth outstanding')
>
>updateTotalDiff :: Persist -> ByteString -> Word64 -> Word64 -> Persist
>updateTotalDiff p bucket t d = p {
>                                 total = insert bucket t $ total p,
>                                 diff  = insert bucket d $ diff p
>                               }

Note that the last bit of that type signature looks rather like the result of *runStateT* on
*StateT Persist IO a*! We can, in fact, use this to build a really convenient function for
updating multiple buckets - all we have to do is explicitly lift it into *StateT Persist IO*
and then use *HashMap*'s built-in traversal-with-applicative-functor function:

>incrementHash :: HashMap ByteString Word64 -> Persist -> IO (HashMap ByteString Word64 , Persist)
>incrementHash = runStateT . traverseWithKey lifted
>    where lifted b w = StateT $ incrementBandwidth b w

Resetting a bucket's bandwidth is rather trivial - just emit a *Set* event and make the relevant modifications to
the state.

>resetBandwidth :: ByteString -> Persist -> IO Persist
>resetBandwidth bucket p = updateTotalDiff p bucket 0 0 <$ writeEvent (Set bucket 0) (handle p)

