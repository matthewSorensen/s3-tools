{-# LANGUAGE OverloadedStrings, DeriveDataTypeable #-}
import Shelly
import Control.Applicative 
import Control.Monad (mapM_)
import Data.Monoid (mappend)
import Filesystem.Path.CurrentOS (encodeString)
import System.Console.CmdArgs
import Control.Monad (unless)
import Data.MIME.Types	(defaultmtd,readMIMETypes,guessType)

import qualified S3sync.Git as Git
import S3sync.S3 hiding (bucket)

data Opts = Opts { reset::Bool
                 , commit::String
                 , bucket::String
                 , access_key::String
                 , secret_key::String
                 } deriving (Show,Data,Typeable)
defaultOpts = Opts {
                reset = False &= help "Executes 'git reset --hard HEAD', empties the S3 bucket, and uploads a copy of HEAD"
              , commit = "" &= help "Git commit message - doesn't commit if this isn't provided"
              , bucket = "" &= help "S3 bucket to upload to"
              , access_key = "" &= help "S3 access key"
              , secret_key = "" &= help "S3 secret"
              } &= summary "s3sync v0.0.1, (c) Matthew Sorensen 2012"

credsFromOpts o = supplementS3Creds $ S3Credentials (access_key o) (secret_key o) $ bucket o

runChanges::S3Credentials->[Git.Change]->ShIO ()
runChanges cred changes = mimeContext >>= flip mapM_ changes . run
    where run mime (Git.Write f)  = notify "Upload" f *> uploadFile cred mime f
          run _    (Git.Delete f) = notify "Delet" f  *> deleteFile cred f
          notify verb f = echo $ verb `mappend` "ing file \"" `mappend` toTextIgnore f `mappend` "\" to S3"
          mimeContext = liftIO $! readMIMETypes defaultmtd False "/etc/mime.types"

resetS3 cred = do
  echo "Cleaning working tree" *> Git.clean
  changes <- Git.everything
  echo "Emptying S3 bucket" *> purgeBucket cred
  runChanges cred changes
syncS3 cred mess = do
  Git.changes >>= runChanges cred
  unless (null mess) (echo "Committing changes" *> Git.commit mess)

main = do
  opts <- cmdArgs defaultOpts
  shelly $ do
         Git.cdToToplevel
         cred <- credsFromOpts opts
         echo "Testing S3 access" *> testS3 cred
         if reset opts then resetS3 cred else syncS3 cred $ commit opts
