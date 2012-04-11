{-# LANGUAGE OverloadedStrings #-}
module S3sync.Utils where

import Shelly 
import Control.Applicative
import Data.Text.Lazy (null,append,unpack)
import Prelude hiding (null)

onFailure err act = catchany_sh act $ const $ terror err

assert _ True = pure ()
assert m False = terror m

void::Applicative f=>f a->f ()
void = (() <$)

env var = do
  val <- getenv var
  if null val
  then terror $ "Environment variable " `append` var `append` " not set"
  else pure $ unpack val
