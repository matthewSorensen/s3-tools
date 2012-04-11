{-# LANGUAGE OverloadedStrings, DeriveDataTypeable #-}
import Shelly 
import Control.Applicative 
import Control.Monad (mapM_)
import Data.Monoid (mappend)
import Filesystem.Path.CurrentOS (encodeString)
import System.Console.CmdArgs
import Control.Monad (unless)

import qualified S3sync.Git as Git
import S3sync.S3

data Opts = Opts {
      reset::Bool,
      commit::String
    } deriving (Show,Data,Typeable)
defaultOpts = Opts {
                reset = False &= help "Executes 'git reset --hard HEAD', empties the S3 bucket, and uploads a copy of HEAD",
                commit = "" &= help "Git commit message - doesn't commit if this isn't provided"
              } &= summary "s3sync v0.0.1, (c) Matthew Sorensen 2012"
runChanges::S3Credentials->[Git.Change]->ShIO ()
runChanges cred = mapM_ run
    where run (Git.Write f)  = notify "Upload" f *> uploadFile cred (encodeString f)
          run (Git.Delete f) = notify "Delet" f *> deleteFile cred (encodeString f)
          notify verb f = echo $ verb `mappend` "ing file \"" `mappend` toTextUnsafe f `mappend` "\" to S3"
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
         cred <- getS3Creds
         echo "Testing S3 access" *> testS3 cred
         if reset opts then resetS3 cred else syncS3 cred $ commit opts
