{-# LANGUAGE OverloadedStrings, FlexibleInstances, OverlappingInstances #-}
module Network.AWS.S3Log.Parser (parseLogs, logParser, safeLogParser) where

import Network.AWS.S3Log.Data

import Prelude hiding (concat)
import Data.Text hiding (concat)
import Data.Attoparsec.Text hiding (parse)
import Control.Applicative
import Data.Char
import Data.Time.Clock (UTCTime)
import Data.Time.Format (parseTime)
import System.Locale (defaultTimeLocale)
import Data.Text.Encoding (decodeUtf8)
import Data.ByteString (ByteString,concat)
import Data.ByteString.Lazy (toChunks)

parseLogs::ByteString->[S3Log]
parseLogs = either (const []) id . parseOnly (many logParser) . decodeUtf8

logParser::Parser S3Log
logParser = fiveteen (three $ pure S3Log) <* rest
    where three f = f <*> parseField <*> parseField <*> parseField
          fiveteen = three . three . three . three . three
          rest = takeTill (=='\n') *> endOfLine

logFailureLine::Parser a
logFailureLine = (takeTill (=='\n') <* endOfLine) >>= fail . ("Failed on: " ++) . unpack

safeLogParser = logParser <|> logFailureLine

class Parse a where
    parse::Parser a

    parseField::Parser a
    parseField = skipSpace *> parse

instance Parse Int where
    parse = decimal <|> (0 <$ char '-')
instance Parse Text where
    parse = takeTill isSpace
instance Parse a=>Parse (Maybe a) where
    parse = (Nothing <$ char '-') <|> (Just <$> parse)
instance Parse Quoted where
    parse = char '"' *> (Quoted <$> scan False sm) <* char '"'
        where sm False '"'  = Nothing
              sm False '\\' = Just True
              sm False _    = Just False
              sm True  _    = Just False
instance Parse (Maybe Quoted) where
    parse = toMaybe <$> parse 
        where toMaybe (Quoted "-") = Nothing
              toMaybe (Quoted t  ) = Just $ Quoted t
instance Parse UTCTime where
    parse = char '[' *> (takeTill (==']') >>= toUTC) <* char ']'
            where toUTC = maybe (fail "timestamp") pure . parseTime defaultTimeLocale "%d/%b/%Y:%H:%M:%S %z" .unpack
instance Parse Requester where
    parse = (Anonymous <$ string "Anonymous") <|> (Requester <$> parse)
