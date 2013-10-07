module TelnetHandler where

import System.IO
import Network.Socket
import Control.Monad

handleTelnet :: (String -> IO String) -> SockAddr -> Handle -> IO ()
handleTelnet runCmd sa h = do
    hPutStr h "> "
    hFlush h
    cmd <- fmap strip $ hGetLine h
    unless (cmd == ":q") $
        do
            out <- runCmd cmd
            hPutStrLn h out
            hPutStrLn h ""
            hPutStrLn h "Enter :q to quit"
            handleTelnet runCmd sa h

strip = lstrip . rstrip
lstrip = dropWhile (`elem` " \t\r\n")
rstrip = reverse . lstrip . reverse
