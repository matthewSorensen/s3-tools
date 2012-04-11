{-# LANGUAGE OverloadedStrings, DeriveDataTypeable #-}

import Shelly hiding ((<$>))
import Control.Applicative hiding (empty)
import Control.Monad

import Filesystem.Path.CurrentOS (encodeString)

import qualified S3sync.Git as Git
import S3sync.S3

import System.Console.CmdArgs

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
    where run (Git.Write f)  = uploadFile cred $ encodeString f
          run (Git.Delete f) = deleteFile cred $ encodeString f

resetS3 cred = do
  Git.clean
  changes <- Git.everything
  purgeBucket cred
  runChanges cred changes

syncS3 cred = Git.changes >>= runChanges cred
  

maybeCommit "" = pure ()
maybeCommit m  = Git.commit m


main = do
  opts <- cmdArgs defaultOpts
  shelly $ do
         Git.cdToToplevel
         cred <- getS3Creds
         testS3 cred
         if reset opts then resetS3 cred else syncS3 cred >> maybeCommit (commit opts)
