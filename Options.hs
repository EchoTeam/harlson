module Options where

import System.Console.GetOpt
import System.Environment

data Mode = StandaloneMode | ErlangPortMode
    deriving Show

data Options = Options
    { optPort       :: !String
    , optTelnetPort :: !String
    , optMode       :: !Mode
    , optSmoothing  :: !Double
    } deriving Show

defaultOptions :: Options
defaultOptions = Options
    { optPort       = "1813"
    , optTelnetPort = "1820"
    , optMode       = StandaloneMode
    , optSmoothing  = 60.0
    }

modeOfString :: String -> Mode
modeOfString sMode = case sMode of
  "erlangport" -> ErlangPortMode
  _        -> StandaloneMode

options :: [OptDescr (Options -> Options)]
options =
    [ Option "p" ["port"]
        (OptArg (optArg $ \opts p -> opts {optPort = p}) "PORT")
        "TCP port for incoming connections (defaults to 1813)"
    , Option "t" ["telnet-port"]
        (OptArg (optArg $ \opts p -> opts {optTelnetPort = p}) "TELNET PORT")
        "TCP port for telnet connections (defaults to 1820)"
    , Option "m" ["mode"]
        (OptArg (optArg $ \opts m -> opts {optMode = modeOfString m}) "MODE")
        "Operational mode: erlangport | standalone (default)"
    , Option "s" ["smoothing"]
        (OptArg (optArg $ \opts s -> opts {optSmoothing = read s}) "SMOOTHING-WINDOW")
        "Smoothing window, secs (defaults to 60.0)"
    ] where optArg f a opts = maybe opts (f opts) a

progOpts :: IO Options
progOpts = do 
  args <- getArgs
  progName <- getProgName
  case getOpt Permute options args of
    (o, _, [])   -> return $ foldl (flip id) defaultOptions o
    (_, _, errs) -> ioError (userError (concat errs ++ usageInfo header options))
      where header = "Usage: " ++ progName ++ " [OPTION...]"
