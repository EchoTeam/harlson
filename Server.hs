-- vim: ts=2 sts=2 sw=2 expandtab
module Server (serveTCP, HandlerFunc) where

-- Code based on the "Real World Haskell" book

import Data.Bits
import Network.Socket
import Network.BSD
import Data.List
import Control.Concurrent
import System.IO

type HandlerFunc = SockAddr -> Handle -> IO ()

serveTCP :: String -> HandlerFunc -> IO ()
serveTCP port handler = withSocketsDo $
  do
    addrinfos <- getAddrInfo
                  (Just (defaultHints {addrFlags = [AI_PASSIVE]}))
                  Nothing (Just port)
    let serveraddr = head addrinfos
    sock <- socket (addrFamily serveraddr) Stream defaultProtocol
    setSocketOption sock ReuseAddr 1
    bindSocket sock (addrAddress serveraddr)
    listen sock 100000 -- max number of simultaneous connections
    procRequests sock
  where
    procRequests :: Socket -> IO ()
    procRequests mastersock = do
      (connsock, clientaddr) <- accept mastersock
      -- setSocketOption connsock NoDelay 1
      forkIO $ procMessages connsock clientaddr
      procRequests mastersock

    procMessages :: Socket -> SockAddr -> IO ()
    procMessages connsock clientaddr = do
      connhdl <- socketToHandle connsock ReadWriteMode
      --hSetBuffering connhdl NoBuffering --(BlockBuffering Nothing)
      handler clientaddr connhdl
      hClose connhdl

