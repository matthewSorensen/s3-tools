{-# LANGUAGE OverloadedStrings #-}
module Network.AWS.S3Log.Parallel (emptyLog,fetchLog,getMaybeDelete) where

import Network.AWS.Monad
import Network.AWS.S3Log.Data (S3Log)
import Network.AWS.S3Log.Parser (parseLogs)

import Network.AWS.AWSConnection (AWSConnection)
import Network.AWS.S3Object (obj_data,S3Object (..),getObject,deleteObject)
import Network.AWS.S3Bucket 

import Control.Monad (when,sequence)
import Control.Monad.IO.Class (liftIO)
import Data.ByteString.Lazy (toChunks)
import qualified Data.ByteString as B
import Data.Either (partitionEithers)
import Control.Concurrent.ParallelIO.Local

emptyLog::AWSConnection->String->IO ([ReqError],[S3Log])
emptyLog = processLogs True

fetchLog::AWSConnection->String->IO ([ReqError],[S3Log])
fetchLog = processLogs False

processLogs::Bool->AWSConnection->String->IO ([ReqError],[S3Log])
processLogs del con bucket = collapse $ do
                               keys <- aws $ listAllObjects con bucket $ ListRequest "" "" "" 1000
                               liftIO  $ parallelRequests $ map (retriveLog del con bucket . key) keys

retriveLog::Bool->AWSConnection->String->String->AWS [S3Log]
retriveLog del con bucket key = fmap parse $ getMaybeDelete del con bucket key
    where parse = parseLogs . B.concat .toChunks . obj_data

getMaybeDelete::Bool->AWSConnection->String->String->AWS S3Object
getMaybeDelete del con bucket key = do
  obj <- aws $ getObject con $ S3Object bucket key "" [] ""
  when del $ aws $ deleteObject con obj
  return obj

parallelRequests::[AWS a]->IO ([ReqError],[a])
parallelRequests =  fmap partitionEithers . withPool 4 . flip parallel . map runEitherT
   
collapse::AWS ([ReqError],[[a]])->IO ([ReqError],[a])
collapse =  fmap (either flail conc )  . runEitherT
    where flail x = ([x],[])
          conc (err,logs) = (err,concat logs)

-- Ultimate function: a conduit that fetches logs in parallel, and preforms