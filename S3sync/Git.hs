{-# LANGUAGE OverloadedStrings #-}
module S3sync.Git (cdToToplevel, -- cd to the root-dir of the current repo
                   commit,     -- Run a commit
                   clean,      -- reset the current repo to HEAD
                   everything, -- All files tracked in HEAD
                   changes,    -- Parses git status, returning a list of changes that are reflected in both the index and working tree
                   Change (..) -- Represents a change to S3
                  ) where
import S3sync.Utils
import Shelly 
import Prelude hiding (init,lines,FilePath)
import Data.Text.Lazy (init,pack,lines,fromChunks)
import Filesystem.Path (FilePath)
import Data.Attoparsec.Text.Lazy
import Control.Applicative

-- A change we need to commit to s3
data Change = Write FilePath | Delete FilePath deriving(Show,Eq)

git command =  silently . run "git" . (command :)

cdToToplevel = do
  dir <- onFailure "Not in a git repository, or git is broken" $ git "rev-parse" ["--show-toplevel"]
  cd $ fromText $ init dir

commit m =  void $ onFailure "Git failed to commit" $ git "commit" ["-m",pack m]

clean = void $ git "reset" ["--hard"]
  
everything::ShIO [Change]
everything = toChanges <$> git "ls-tree" ["-r","--name-only","HEAD"]
    where toChanges = fmap (Write . fromText)  . lines

changes::ShIO [Change]
changes = do
  status <- git "status" ["-z"]
  case parse (parseEntry `manyTill` end) status of
    Done _ entries -> pure (entries >>= selectChange)
    Fail _ _ _ -> terror "Failed to parse git status output"
-- A parser for git-status' machine output format (-z), documented at http://schacon.github.com/git/git-status.html
-- Makes the (rather reasonable, but not totally documented) assumption that the two-file path record will only show
-- up in the case of a move in either the index or working tree.
data GitEntry = GitEntry {
      index::Action,
      work::Action,
      path::FilePath,
      other::Maybe FilePath
    } deriving (Show,Eq)
data Action = None | Modified | Added | Deleted | Renamed | Copied | Unmerged | Ignore deriving(Show,Eq)

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

nul = char '\NUL'
end = endOfLine >> endOfInput
filepathFromStrict = fromText . fromChunks . (:[])

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