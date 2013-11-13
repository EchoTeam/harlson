module TelnetHandler where

import System.IO
import Network.Socket
import Control.Monad
import Paths_harlson (version)
import Data.Version (showVersion)
import Data.Char

handleTelnet :: (String -> IO String) -> SockAddr -> Handle -> IO ()
handleTelnet runCmd sa h = do
    hPutStrLn h "\n"
    hPutStrLn h ("harlson " ++ showVersion version)
    handleTelnet' runCmd sa h    

handleTelnet' :: (String -> IO String) -> SockAddr -> Handle -> IO ()
handleTelnet' runCmd sa h = do
    hPutStrLn h "Ctrl-D to quit"
    hPutStr h "> "
    hFlush h
    c <- hLookAhead h
    unless (c == chr 4) $
        do  
            cmd <- fmap strip $ hGetLine h
            unless (cmd == ":q" || cmd == [chr 4]) $
                do
                    out <- runCmd cmd
                    hPutStrLn h out
                    hPutStrLn h ""
                    handleTelnet' runCmd sa h

strip :: String -> String
strip = lstrip . rstrip where
    lstrip = dropWhile (`elem` " \t\r\n")
    rstrip = reverse . lstrip . reverse
