{-# LANGUAGE OverloadedStrings #-}
module Network.AWS.S3Log.Parallel (emptyLog,fetchLog) where

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
retriveLog del con bucket key = do
  log <- aws $ getObject con $ S3Object bucket key "" [] ""
  when del $ aws $ deleteObject con log
  return $ parseLogs $ B.concat $ toChunks $ obj_data log

parallelRequests::[AWS a]->IO ([ReqError],[a])
parallelRequests =  fmap partitionEithers . withPool 4 . flip parallel . map runEitherT
   
-- parallelRequests =  fmap partitionEithers . parallel . map runEitherT
   
collapse::AWS ([ReqError],[[a]])->IO ([ReqError],[a])
collapse =  fmap (either flail conc )  . runEitherT
    where flail x = ([x],[])
          conc (err,logs) = (err,concat logs)
{--

retriveLogs::AWSConnection->String->String->AWS [S3Log]
retriveLogs c bucket key = do
  log <- aws $ getObject c $ S3Object bucket key "" [] ""
  aws $ deleteObject c log
  return $ logMessages $ concat $ toChunks $ obj_data log


emptyLogBucket::AWSConnection->String->IO (AWSResult [[S3Log]])
emptyLogBucket c buck = runEitherT $ aws (listAllObjects c buck para) >>= mapM fetch
    where fetch (ListResult k _ _ _ _) = retriveLogs c buck k
          para  = ListRequest "" "" "" 1000

--}
