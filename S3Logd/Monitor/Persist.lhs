---
title: Simple Append-only/Log-structured Persistence
author: Matthew Sorensen
published: August 3, 2012
short: Wherein the construction of the storage layer for S3Logd is detailed.
tags: Haskell
---

Append-only persistance layer.

Implements a simple log-based persistance layer for current bandwidth consumption. 
Aims to be decently performant, somewhat space-efficient, and suffer from minimal 
data-loss upon crashes.

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
>import Control.Monad.Trans (liftIO)
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

Why does this matter?
             
>type Granularity = Word64

>data Persist = Persist {
>      total :: HashMap ByteString Word64,
>      diff :: HashMap ByteString Word64,
>      path :: FilePath,
>      handle :: Handle,
>      granularity :: Word64
>    } deriving (Show)
>
>writeEvent :: LogEvent -> Handle -> IO ()
>writeEvent l h = hPut h $ serialize l

Initialization
---------

>replay :: [LogEvent] -> HashMap ByteString Word64
>replay = foldl' (flip add) empty
>    where add (Increment name inc) = adjust (+inc) name
>          add (Set name value)     = insert name value

>writeState :: Handle -> (ByteString -> Word64 -> LogEvent) -> HashMap ByteString Word64 -> IO ()
>writeState h con hash = traverseWithKey logIfNonZero hash >> return ()
>    where logIfNonZero bucket band
>              | band > 0 = writeEvent (con bucket band) h
>              | otherwise = return ()


>openPersist :: FilePath -> Granularity -> IO (Either String Persist)
>openPersist file gran = do
>  let backup = file <.> "backup"
>  fExists <- doesFileExist file
>  bExists <- doesFileExist backup
>  case (fExists,bExists) of
>           (True,True)  -> removeFile file
>           (False,True) -> return ()
>           (True,False) -> renameFile file backup
>           (False,False)-> writeFile backup ""
>  log <- fmap deserialize $ readFile backup
>  case log of
>           Left s -> return $ Left s
>           Right log -> do
>             let state = replay log
>             handle <- openFile file AppendMode
>             hSetBuffering handle NoBuffering
>             writeState handle Set state
>             removeFile backup
>             return $ Right $ Persist state empty file handle gran

Destruction
---------

Closing a session is a matter of writing out all outstanding diffs, then
closing the file, then deleting any possible backups.

>closePersist :: Persist -> IO ()
>closePersist p = do
>  writeState (handle p) Increment $ diff p
>  hClose $ handle p
>  let backup = path p <.> "backup"
>  exists <- doesFileExist backup
>  when exists $ removeFile backup


Tricky parts
------

>updateTotalDiff :: Persist -> ByteString -> Word64 -> Word64 -> Persist
>updateTotalDiff p bucket t d = p {
>                                 total = insert bucket t $ total p,
>                                 diff  = insert bucket d $ diff p
>                               }

Why have we generalized this to *MonadIO* instead of just IO? Time will. Tell.

>incrementBandwidth :: (MonadIO m, Functor m) => ByteString -> Word64 -> Persist -> m (Word64 , Persist)
>incrementBandwidth bucket new p = maybe insertBucket bucketExists $ lookup bucket $ total p
>    where insertBucket = do
>                         liftIO $ writeEvent (Set bucket new) (handle p)
>                         return $! (new, updateTotalDiff p bucket new 0)           
>          bucketExists exists = do
>                         let diff' = new + lookupDefault 0 bucket (diff p)
>                         if diff' > granularity p 
>                         then (diff' + exists, updateTotalDiff p bucket (diff' + exists) 0)
>                                <$ liftIO (writeEvent (Increment bucket diff') (handle p))
>                         else return (exists + diff', updateTotalDiff p bucket exists diff')

Note that the last bit of that type signature looks rather like the result of runStateT on
*StateT Persist IO a*! We can, in fact, use this to build a really convienient function for
updating multiple buckets - all we have to do is explicitly lift it into *StateT Persist IO a*
and then use *HashMap*'s built-in traversal-with-applicative-functor function:

>incrementHash :: HashMap ByteString Word64 -> Persist -> IO (HashMap ByteString Word64 , Persist)
>incrementHash = runStateT . traverseWithKey lifted
>    where lifted b w = StateT $ incrementBandwidth b w



>resetBandwidth :: ByteString -> Persist -> IO Persist
>resetBandwidth bucket p = updateTotalDiff p bucket 0 0 <$ writeEvent (Set bucket 0) (handle p) 


Testing Interlude
-----------------

So how do we test *incrementBandwidth*? 

>newtype Ignore a = Ignore a deriving (Show)
>
>instance Monad Ignore where
>    return = Ignore
>    (Ignore a) >>= f = f a
>
>instance Functor Ignore where
>    fmap f (Ignore a) = Ignore $ f a 
>instance MonadIO Ignore where
>    liftIO _ = Ignore $ error "No peaking!"

Then implement quickcheck properties in the Ignore monad instead of IO.