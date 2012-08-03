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
> module S3Logd.Monitor.Persist (
>                                Persist,
>                                updateBandwidth,
>                                resetBandwidth,
>                                openPersist,
>                                closePersist
>                               ) where
>
> import Data.ByteString hiding (empty,take,map,foldl')
> import Data.HashMap.Strict hiding (foldl',map)
> import System.IO hiding (writeFile,readFile)
> import Control.Monad.State
> import Control.Monad.Trans (liftIO)
> import Prelude hiding (lookup,foldl,take,all,length,writeFile,readFile)
> import Data.List (foldl')
> import System.Directory (doesFileExist,renameFile,removeFile)
> import Control.Applicative hiding (empty)
> import Blaze.ByteString.Builder.Int
> import Blaze.ByteString.Builder.ByteString
> import Blaze.ByteString.Builder.Char8 (fromChar)
> import Blaze.ByteString.Builder
> import Data.Attoparsec.ByteString
> import Data.Attoparsec.Combinator
> import Data.Monoid (mappend,mconcat)
> import Data.Bits
> import Data.Word
> import Test.QuickCheck hiding (replay)
> import System.FilePath

 ## Events

Fortunately, S3Logd's storage requirements are really very minimal: we (currently) only need to
store a natural number representing the number of bytes transferred in and out of an S3 bucket over 
a period of time. Each record thus only needs to support two possible types of mutation, namely 
being reset to a specific value and being increment by a value, which gives a simple type for
log events:

> data LogEvent = Increment ByteString Word64 | Set ByteString Word64
>                 deriving(Eq,Show)

The first order of business is efficent serialization and deserialization. While the *cereal* or
*binary* packages could be used to do all of this more-or-less automatically, I've choosen to use
[*blaze-builder*](http://hackage.haskell.org/package/blaze-builder-0.3.0.2) and 
[*attoparsec*](http://hackage.haskell.org/package/attoparsec-0.10.2.0), in the theory that handling a 
possibly-incomplete record at the end of the log is much easier with attoparsec. The actual format used
isn't particularly important, so I've more-or-less abitrarility choosen *{'i' or 's' tag byte} NULL 
{bytestring} NULL {big-endian 64-bit word} NULL* . This does assume that bucket names have no null-bytes 
- this is checked elsewhere and such a restriction is prefereable to an 
[explicit length-field](http://www.cs.dartmouth.edu/~sergey/langsec/insecurity-theory-28c3.pdf).

> serialize :: LogEvent -> ByteString
> serialize (Set name size) = ser' "s\NUL" name size
> serialize (Increment name size) = ser' "i\NUL" name size
>
> ser' tag name int = toByteString $ fromByteString tag  `mappend` 
>                     fromByteString name `mappend` fromChar '\NUL' `mappend`
>                     fromWord64be int    `mappend` fromChar '\NUL'
>
> deserialize :: ByteString -> Either String [LogEvent]
> deserialize = parseOnly $ many (tag <*> name <*> size)
>     where tag = (Set <$ string "s\NUL") <|> (Increment <$ string "i\NUL")
>           name = takeWhile1 (/= 0) <* word8 0
>           size = foldl  orShiftL 0 <$> take 8 <* word8 0
>           orShiftL word64 word8 = (word64 `shiftL` 8) .|. toEnum (fromEnum word8)

 ## Testing Interlude

There are a few properties of serialization/deserialization we'd like to verify: first and
foremost, *deserialize* must be the inverse of *serialize* for all *LogEvents*. Furthermore,
if we deliberately corrupt/truncate the last few bits of a serialized list of *LogEvents*, we
should still be able to parse all events except the last. In order to verify this like a "real
Haskeller" (ie. someone too lazy to write unit tests), we turn to QuickCheck.

> instance Arbitrary LogEvent where
>     arbitrary = (tag <$> arbitrary) <*> name <*> arbitrary
>         where tag True = Set
>               tag False = Increment
>               name = (pack <$> arbitrary) `suchThat` \x -> all (/= 0) x && length x > 0
>
> prop_inv :: LogEvent -> Bool
> prop_inv l = Right [l] == deserialize (serialize l)
>
> prop_corrupt :: [LogEvent] -> [Word8] -> Bool
> prop_corrupt l bs = Right l == deserialize (mconcat $ (map serialize l) ++ [pack bs])
  
 ## Persist Datatype

Why does this matter?
             
> type Granularity = Word64

> data Persist = Persist {
>       total :: HashMap ByteString Word64,
>       diff :: HashMap ByteString Word64,
>       path :: FilePath,
>       handle :: Handle,
>       granularity :: Word64
>     } 
>
> writeEvent :: LogEvent -> Handle -> IO ()
> writeEvent l h = hPut h $ serialize l

 ## Initialization

> replay :: [LogEvent] -> HashMap ByteString Word64
> replay = foldl' (flip add) empty
>     where add (Increment name inc) = adjust (+inc) name
>           add (Set name value)     = insert name value

> writeState :: Handle -> HashMap ByteString Word64 -> IO ()
> writeState h hash = traverseWithKey logIfNonZero hash >> return ()
>     where logIfNonZero bucket band
>               | band > 0 = writeEvent (Set bucket band) h
>               | otherwise = return ()

Algorithm for opening is rather simple, then:
-- ensure the directory is in the following state:
   - backup file exists
   - non-backup file is empty
-- replay the contents of the backup file into a hash
-- write the hash into the non-backup file
-- delete the backup file
-- done
Make sure to set buffering to off.

> openPersist :: FilePath -> Granularity -> IO (Either String Persist)
> openPersist file gran = do
>   let backup = file <.> "backup"
>   fExists <- doesFileExist file
>   bExists <- doesFileExist backup
>   case (fExists,bExists) of
>     (True,True)  -> removeFile file
>     (False,True) -> return ()
>     (True,False) -> renameFile file backup
>     (False,False)-> writeFile backup ""
>   state <- fmap (fmap replay . deserialize) $ readFile backup
>   either (return . Left) (fmap Right . openWriteClean file backup gran) state

> openWriteClean :: FilePath -> FilePath -> Granularity -> HashMap ByteString Word64 -> IO Persist
> openWriteClean f backup g m = do
>   handle <- openFile f AppendMode
>   hSetBuffering handle NoBuffering
>   writeState handle m
>   removeFile backup
>   return $! Persist m empty f handle g








  --  import System.Directory (doesFileExist,renameFile,removeFile)


> mungeFiles :: FilePath -> IO (FilePath,Handle)
> mungeFiles = undefined


 ## Destruction


 ## Tricky parts


Takes a map of new bandwidth, persists the changes, and then returns a map of
total bandwidth.

This really isn't a very functional solution to the provlem, but it is certainly elegant.
We can do something horridly awesome with StateT Persist IO and traverseWithKey

> updateBandwidth :: HashMap ByteString Word64 -> Persist -> IO (HashMap ByteString Word64,Persist)
> updateBandwidth =  runStateT . traverseWithKey foldFunction
>     where foldFunction bucket band = do
>                                      p <- get
>                                      maybe (new p bucket band) (existing p bucket band) $ lookup bucket $ total p

If the bucket doesn't exist in the set of totals, emit a {write value}, set the total to value, and diff to 0.

>           new p bucket band = do
>                               liftIO $ writeEvent (Set bucket band) (handle p)
>                               put $ p { total = insert bucket band $ total p, diff = insert bucket 0 $ diff p}
>                               return band

Otherwise, we may or may not persist the update, depending on granularity...
                 
>           existing p bucket band totalBand = do
>                                              let tot = band + lookupDefault 0 bucket (diff p)
>                                              case tot > granularity p of
>                                                                       True -> do
>                                                                               liftIO $ writeEvent (Increment bucket tot) (handle p)
>                                                                               put $ p { total = insert bucket (totalBand + tot) $ total p,
>                                                                                         diff = insert bucket 0 $ diff p}
>                                                                       False -> do
>                                                                                put $ p { diff = insert bucket tot $ diff p}
>                                              return $! tot + totalBand
                
> resetBandwidth :: ByteString -> Persist -> IO Persist
> resetBandwidth  name p = do
>                          writeEvent (Set name 0) (handle p)
>                          return $! p { total = insert name 0 $ total p, diff = insert name 0 $ diff p }


this is the tricky bit.
If a backup file exists, nuke the non-backup file and use the backup
Otherwise, copy the current file to the backup, then delete the old file3

Closing the store is then a matter of persisting all of the remaining diffs,
closing the file handle, and deleting any backup.

> closePersist :: Persist -> IO ()
> closePersist = undefined

-- First, a nice wire protocol for log events:


{--

{i,s}NULL{bytestring name}NULL{big-endian 64-bit int}NULL

--}


