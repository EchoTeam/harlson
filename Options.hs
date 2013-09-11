module Options where

import System.Console.GetOpt
import System.Environment

data Mode = StandaloneMode | ErlangPortMode
    deriving Show

data Options = Options
    { optPort       :: String
    , optMode       :: Mode
    } deriving Show

defaultOptions = Options
    { optPort = "1813"
    , optMode = StandaloneMode
    }

modeOfString :: String -> Mode
modeOfString sMode = case sMode of
  "erlangport" -> ErlangPortMode
  _        -> StandaloneMode

options :: [OptDescr (Options -> Options)]
options =
    [ Option ['p'] ["port"]
        (OptArg (optArg $ \opts p -> opts {optPort = p}) "PORT")
        "TCP port for incoming connections (defaults to 1813)"
    , Option ['m'] ["mode"]
        (OptArg (optArg $ \opts m -> opts {optMode = modeOfString m}) "MODE")
        "Operational mode: erlangport | standalone (default)"
    ] where optArg f a opts = maybe opts (\m -> f opts m) a

progOpts :: IO Options
progOpts = do 
  args <- getArgs
  progName <- getProgName
  case getOpt Permute options args of
    (o, n, [])   -> return $ foldl (flip id) defaultOptions o
    (_, _, errs) -> ioError (userError (concat errs ++ usageInfo header options))
      where header = "Usage: " ++ progName ++ " [OPTION...]"
