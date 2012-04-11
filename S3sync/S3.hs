{-# LANGUAGE OverloadedStrings #-}
module S3sync.S3 (getS3Creds, 
                  testS3,
                  deleteFile,
                  uploadFile,
                  purgeBucket,
                  S3Credentials) where
import S3sync.Utils 

import Shelly hiding ((<$>))
import Control.Applicative hiding (empty)
import Data.Text.Lazy 
import Prelude hiding(init,null,readFile)
import Data.Monoid
import Data.ByteString.Lazy hiding(pack)
import Data.MIME.Types	(defaultmtd,guessType)
import Filesystem.Path.CurrentOS (decodeString,encodeString)
import Codec.Compression.GZip (compress)
import Network.AWS.AWSConnection
import Network.AWS.S3Bucket
import Network.AWS.AWSResult
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
      access_key::String,
      secret_key::String,
      bucket::String
    } deriving (Show,Eq)
getS3Creds::ShIO S3Credentials
getS3Creds = S3Credentials <$> env "AWS_ACCESS_KEY" <*> env "AWS_SECRET_KEY" <*> env "AWS_S3_BUCKET"

connectionFromCreds c = amazonS3Connection (access_key c) $ secret_key c

amazonError = terror . mappend "AWS error: " . pack . prettyReqError

runAWS::IO (AWSResult a)->ShIO ()
runAWS act = liftIO act >>= (() <$) . either amazonError pure

s3object::S3Credentials->FilePath->S3Object
s3object buck f = S3Object {
             obj_bucket = bucket buck,
             obj_name = f,
             content_type = "binary/octet-stream",
             obj_headers = mempty,
             obj_data = mempty
           }
serverHeaders obj = obj {
                      content_type = getMimeType $ obj_name obj,
                      obj_headers = gzip : cache : obj_headers obj
                    } where gzip = ("x-amz-meta-Content-Encoding","gzip")
                            cache = ("x-amz-meta-Cache-Control","max-age=21600") -- Cache everything for 6 hours.
                            getMimeType = maybe "binary/octet-stream" id . fst . guessType defaultmtd False 
readContent::S3Object->ShIO S3Object
readContent obj = do
  dat <- path (decodeString $ obj_name obj) >>= liftIO . readFile .  encodeString
  pure $ obj {obj_data = compress dat}
