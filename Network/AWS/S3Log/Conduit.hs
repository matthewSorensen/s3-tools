module Network.AWS.S3Log.Conduit where

import Network.AWS.S3Log.Parser (safeLogParser)
import Network.AWS.S3Log.Data hiding (key)
import Network.AWS.S3Log.Parallel (getMaybeDelete)
import Network.AWS.Monad

import Network.AWS.AWSConnection (AWSConnection)
import Network.AWS.S3Object (obj_data,S3Object)
import Network.AWS.S3Bucket (ListRequest (..), key, listAllObjects)
import Network.AWS.AWSResult (AWSResult,ReqError)

import Data.Conduit
import Data.Conduit.Text
import Data.Conduit.Internal (Finalize (FinalizePure))
import Control.Monad.IO.Class (liftIO)

import Data.ByteString.Lazy (ByteString,toChunks)
import qualified Data.ByteString as B
import Data.Text (Text)

import qualified Data.Attoparsec.Text as A

import Data.Conduit.Lazy
import Data.Conduit.List (sourceList)


data BucketState = Keys [String] | Error ReqError | Terminate
type ResourceIO = ResourceT IO

enumerateBucket::Bool->AWSConnection->String->Source ResourceIO (AWSResult S3Object)
enumerateBucket del con bucket = sourceStateIO init none step
    where none _ = return () -- We have no cleanup to perform
          -- Fetch a list of all the objects:
          init = fmap toBucketState $ listAllObjects con bucket $ ListRequest "" "" "" 1000
          toBucketState = either Error (Keys . map key)
          -- Fetch each object                                 
          step Terminate = return $ StateClosed
          step (Error e) = return $ StateOpen Terminate (Left e)
          step (Keys []) = return $ StateClosed
          step (Keys (x:rest)) = liftIO $ fmap (StateOpen $ Keys rest) $ runEitherT $ getMaybeDelete del con bucket x

toText::(Monad m, MonadThrow m)=>Conduit ByteString m Text
toText =  conduitState () push none =$= decode utf8
    where none _   = return []
          push _ i = return $ StateProducing () $ toChunks i

data ParseState a  = Finished | Parsing (Text->A.Result a)
type ParseResult a = Either String a

-- Critically, the supplied parser must ALWAYS consume input upon failure - otherwise the conduit will return
-- an infinite stream of parse errors. Ideally, it consumes all of the invalid input - although this is clearly
-- a rather difficult task for any complex grammars.

attoparsec::(Monad m,MonadThrow m)=>A.Parser a->Conduit Text m (ParseResult a)
attoparsec parser = conduitState Finished push flush
    where consumeChunk acc st "" = (st, reverse acc)
          consumeChunk acc st t  = case (getCont st) t of
                                     (A.Done rest new) -> consumeChunk (Right new : acc) Finished rest
                                     (A.Partial f)       -> (Parsing f, reverse acc)
                                     (A.Fail rest _ err) -> consumeChunk (Left  err : acc) Finished rest 

          getCont Finished    = A.parse parser
          getCont (Parsing f) = f

          push = ((return . uncurry StateProducing) .) . consumeChunk []

          flush Finished    = return []
          flush (Parsing f) = return $ case f "" of
                                         (A.Done _ new) -> [Right new]  
                                         (A.Partial _)    -> [Left "End of input"]
                                         (A.Fail _ _ err) -> [Left err] 

-- Now for the whole point of this file
streamLogEntries::Bool->AWSConnection->String->Source ResourceIO (Either String S3Log)
streamLogEntries = undefined


-- I really really don't think I'm supposed to be able to do this. But I can. 
liftToEither::Functor m=>Conduit i m o->Conduit (Either a i) m (Either a o)
liftToEither (HaveOutput new final out) = HaveOutput (liftToEither new) final $ Right out
liftToEither (NeedInput cont early) = let lifted = NeedInput newCont $ liftToEither early
                                          final  = FinalizePure ()
                                          newCont = either (HaveOutput lifted final . Left ) (liftToEither . cont)
                                      in lifted
liftToEither (Done m r) = Done (fmap Right m) r
liftToEither (PipeM act f) = PipeM (fmap liftToEither act) f
