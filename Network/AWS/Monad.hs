module Network.AWS.Monad (EitherT (..), ReqError, AWS, aws) where

import Network.AWS.AWSResult (ReqError,AWSResult)
import Control.Monad.IO.Class

-- EitherT monad transformer - pretty typical stuff.
newtype EitherT m a b = EitherT {runEitherT:: m (Either a b)}

instance Functor m=>Functor (EitherT m a) where
    fmap f = EitherT . fmap (fmap f) . runEitherT

instance Monad m=>Monad (EitherT m a) where
    return = EitherT . return . return
    x >>= f = EitherT $ runEitherT x >>= either (return . Left) (runEitherT . f)

-- The AWS operations then live in EitherT IO ReqError
type AWS = EitherT IO ReqError

aws::IO (AWSResult a)->AWS a
aws = EitherT

instance MonadIO m=> MonadIO (EitherT m a) where
    liftIO = EitherT . liftIO . fmap Right