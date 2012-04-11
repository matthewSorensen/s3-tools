{-# LANGUAGE OverloadedStrings #-}
module S3sync.Git (cdToToplevel, commit, clean, everything, Change (..)) where

import S3sync.Utils
import Shelly hiding ((<$>))
import Prelude hiding (init,lines,FilePath)
import Data.Text.Lazy hiding (index,all)
import Control.Applicative
import Filesystem.Path
import Control.Monad
import Data.Attoparsec.Text.Lazy

git command =  run "git" . (command :)

-- cd to the top directory of the current git repository.
-- verifies that we're in a git repo and that git works.
cdToToplevel::ShIO ()
cdToToplevel = do
  dir <- onFailure "Not in a git repository, or git is broken" $ silently $ git "rev-parse" ["--show-toplevel"]
  cd $ fromText $ init dir

commit::Text->ShIO ()
commit m =  () <$ (onFailure "Git failed to commit" $ silently $ git "commit" ["-m",m])

-- Ensure that the working directory is clean. Only needed for completely re-uploading the repo.
clean::ShIO ()
clean = () <$ (silently $ git "reset" ["--hard"])
  
everything::ShIO [Change]
everything = toChanges <$> git "ls-tree" ["-r","--name-only","HEAD"]
    where toChanges = fmap (Write . fromText)  . lines

-- A change we need to commit to s3
data Change = Write FilePath | Delete FilePath deriving(Show,Eq)

changes::ShIO [Change]
changes = do
  status <- git "status" ["-z"]
  case parse (parseEntry `manyTill` end) status of
    Done _ entries -> pure (entries >>= selectChange)
    Fail _ _ _ -> terror "Failed to parse git status output"

end = endOfLine >> endOfInput

-- A parser for git-status' machine output format (-z), from http://schacon.github.com/git/git-status.html

data GitEntry = GitEntry {
      index::Action,
      work::Action,
      path::FilePath,
      other::Maybe FilePath
    } deriving (Show,Eq)

data Action = None | Modified | Added | Deleted | Renamed | Copied | Unmerged | Ignore deriving(Show,Eq)

nul = char '\NUL'

filepathFromStrict = fromText . fromChunks . (:[])

parseEntry::Parser GitEntry
parseEntry = (GitEntry <$> action <*> action <*> parsePath <*> pure Nothing) >>= otherFile
    where otherFile entry
              | (Renamed == index entry) || (Renamed == work entry)  = setOther entry <$> parsePath <* nul
              | otherwise = entry <$ nul
          parsePath = (char ' ' <|> nul) *> (filepathFromStrict <$> takeTill (=='\NUL'))
          setOther x y = x {other = Just y}

action = choice $ mp <$> [(None,' '),(Modified, 'M'),
                          (Added,'A'),(Deleted,'D'),
                          (Renamed,'R'),(Copied,'C'),
                          (Unmerged,'U'),(Ignore,'!'),(Ignore,'?')]
    where mp (act,c) = act <$ char c

-- Only take the changes that we want - and rewrite all moves and copies in terms of add/delete.
selectChange::GitEntry->[Change]
selectChange e = if all ($ e) [tracked, unmodified, noConflicts] then rewrite e else []
    where both f (GitEntry a b _ _) = f a && f b
          tracked = both (/= Ignore)
          -- The file in the working tree must match the index.
          unmodified (GitEntry _ None _ _ ) = True
          unmodified _ = False
          -- This is picky, but any file that has merge conflicts shouldn't live.
          noConflicts = both (/= Unmerged)
          -- Rewrite the status entry in terms of actions we can take with s3:
          rewrite (GitEntry Added _ f _) = [Write f]
          rewrite (GitEntry Deleted _ f _) = [Delete f]
          rewrite (GitEntry Modified _ f _) = [Write f]
          rewrite (GitEntry Copied _ f _) = [Write f]
          rewrite (GitEntry Renamed _ to (Just from)) = [Delete from, Write to]
          rewrite _ = []
