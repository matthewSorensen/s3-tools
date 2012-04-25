{-# LANGUAGE TemplateHaskell #-}
module S3Logd.Config (
                      Config (..),
                      Credentials (..),
                      Paths (..),
                      loopWithConf
                     ) where

import S3Logd.Syslog (syslog)

import Prelude hiding (readFile)

import Data.Aeson
import Data.Aeson.TH

import Data.ByteString.Lazy (readFile)
import Data.Map (Map)

import Control.Concurrent.MVar
import System.Posix.Signals
import Control.Exception (try,SomeException (..))

import Control.Monad (forever)

loopWithConf::FilePath->(Config->IO ())->IO ()
loopWithConf path f = do
  m <- newEmptyMVar
  installHandler sigHUP (Catch $ reloadConfig path m) $ Just emptySignalSet
  reloadConfig path m
  forever $ readMVar m >>= f

-- Read the file, parse it. If anything goes wrong send a message to syslog.
-- Otherwise, put the new conf in the MVar.
reloadConfig::FilePath->MVar Config->IO ()
reloadConfig file m = do
  conf <- fmap (>>= decode) $  maskAll $ readFile file
  maybe failure (fmap (const ()) . swapMVar m) conf
      where failure = syslog "Error loading config file"


maskAll::IO a->IO (Maybe a)
maskAll = fmap (either (ignore::SomeException->Maybe a) Just) . try
    where ignore = const Nothing



data Config = Config {
      interval::Int,
      credentials::Credentials,
      paths::Paths,
      log_formats::Maybe [String],
      bandwidth_limits::Maybe (Map String Int)
      } deriving(Show)

data Credentials = Credentials {
      access_key::String,
      secret_key::String
    } deriving(Show)

data Paths = Paths {
      log_directory::FilePath,
      state::FilePath
    } deriving(Show)

$(deriveJSON (map (\x->if x=='-' then ' ' else x)) ''Config)
$(deriveJSON (map (\x->if x=='-' then ' ' else x)) ''Credentials)
$(deriveJSON (map (\x->if x=='-' then ' ' else x)) ''Paths)
