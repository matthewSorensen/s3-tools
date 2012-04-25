module S3Logd.Syslog (syslogLefts, consoleLefts,panic,syslog) where

import Data.Conduit
import Control.Applicative
import Control.Monad.IO.Class (liftIO)

import System.Log.Handler
import System.Log.Handler.Syslog
import System.Log

-- Nom on all the lefts:
splitLefts::(MonadResource m)=> IO st -> (st->IO ()) -> (st->b->IO ()) -> Conduit (Either b a) m a
splitLefts aloc deloc consume = conduitIO aloc deloc push none
    where none _ = return []
          push st = either (io st) wrap
          io st b = do
            liftIO $ consume st b
            return $ IOProducing []
          wrap a = return $ IOProducing [a]

-- Treat all the lefts as errors that need to be sent to syslog
syslogLefts::(MonadResource m)=>Conduit (Either String a) m a
syslogLefts = splitLefts logger close undefined
    where logger = openlog "s3logd" [PID] DAEMON ERROR
          log st str = handle st (ERROR,str) "error" 

-- Just print all lefts to the console.
consoleLefts::(MonadResource m)=>Conduit (Either String a) m a
consoleLefts =  splitLefts void none (const $ putStrLn)
    where void = return ()
          none _ = return ()


-- Log a string to syslog as an EMERGENCY
panic::String->IO ()
panic = logError EMERGENCY

syslog = logError ERROR

logError level str = do
  logger <- openlog "s3logd" [PID] DAEMON ERROR
  handle logger (level, str) "panic"
  close logger