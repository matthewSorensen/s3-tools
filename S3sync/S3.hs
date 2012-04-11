{-# LANGUAGE OverloadedStrings #-}
module S3sync.S3 (supplementS3Creds,
                  testS3,
                  deleteFile,
                  uploadFile,
                  purgeBucket,
                  S3Credentials (..)) where
import S3sync.Utils 

import Shelly 
import Prelude hiding (readFile)
import Control.Applicative
import Data.Monoid (mempty,mappend)
import Data.Maybe (fromMaybe)
import Data.MIME.Types	(defaultmtd,guessType)
import Filesystem.Path.CurrentOS (decodeString,encodeString)
import Codec.Compression.GZip (compress)
import Data.ByteString.Lazy (readFile)
import Data.Text.Lazy (pack)
import Network.AWS.AWSConnection (amazonS3Connection)
import Network.AWS.S3Bucket (getBucketLocation, emptyBucket)
import Network.AWS.AWSResult (AWSResult, prettyReqError)
import Network.AWS.S3Object 

-- The actual S3 commands we want:
-- Checks that we can access S3 with the current credentials, and that the bucket we're using exists.
testS3::S3Credentials->ShIO ()
testS3 c = runAWS $ getBucketLocation (connectionFromCreds c) $ bucket c

purgeBucket::S3Credentials->ShIO ()
purgeBucket c = runAWS $ emptyBucket (connectionFromCreds c) $ bucket c
 
deleteFile::S3Credentials->FilePath->ShIO ()
deleteFile c f = runAWS $ deleteObject (connectionFromCreds c) $ s3object c f

uploadFile::S3Credentials->FilePath->ShIO ()
uploadFile cred f = do
  object <- readContent $ setStorageClass STANDARD $ serverHeaders $ s3object cred f
  runAWS $ sendObject (connectionFromCreds cred) object

data S3Credentials = S3Credentials {
      accessKey::String,
      secretKey::String,
      bucket::String
    } deriving (Show,Eq)

-- Takes a set of credentials and fills them missing fields from environment variables
supplementS3Creds::S3Credentials -> ShIO S3Credentials
supplementS3Creds (S3Credentials acc sec buc) = S3Credentials <$> supEnv acc "AWS_ACCESS_KEY" <*> supEnv sec "AWS_SECRET_KEY" <*> supEnv buc "AWS_S3_BUCKET"
    where supEnv "" key = env key
          supEnv x  _   = pure x

connectionFromCreds (S3Credentials access secret _) = amazonS3Connection access secret

runAWS::IO (AWSResult a)->ShIO ()
runAWS = (() <$  ) . (>>= either awsError pure) . liftIO
    where awsError = terror . mappend "AWS error: " . pack . prettyReqError

s3object::S3Credentials->FilePath->S3Object
s3object buck f = S3Object {
                    obj_bucket = bucket buck
                  , obj_name = f
                  , content_type = "binary/octet-stream"
                  , obj_headers = mempty
                  , obj_data = mempty }
serverHeaders obj = obj { content_type = getMimeType $ obj_name obj
                        , obj_headers = gzip : cache : obj_headers obj } 
    where gzip = ("x-amz-meta-Content-Encoding","gzip")
          cache = ("x-amz-meta-Cache-Control","max-age=21600") -- Cache everything for 6 hours.
          getMimeType = fromMaybe "binary/octet-stream" . fst . guessType defaultmtd False 

readContent::S3Object->ShIO S3Object
readContent obj = do
  dat <- path (decodeString $ obj_name obj) >>= liftIO . readFile .  encodeString
  pure $ obj {obj_data = compress dat}