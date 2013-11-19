import Network.Socket
import Network.BSD
import Control.Monad
import Control.Concurrent
import Control.Exception
import System.IO
import qualified Data.Binary.Put as Put
import qualified Data.Binary.Get as Get
import qualified Data.ByteString.Lazy as B
import qualified Data.ByteString.Lazy.Char8 as B8

import Query

main = do
    runJob 1 (updateMetrics 10000)
    runJob 1 updateLimits
    vars <- replicateM 300 $ myForkIO $ runJob 1 (updateMetrics 2000)
    mapM_ takeMVar vars
    print "Done put"
    runJob 10 getOverLimit
    runJob 1 stop

runJob n job = do
    h <- newConnHandle
    replicateM_ n $ job h
    hClose h

metrics :: [QMetricLevel]
metrics = [QMetricLevel (B8.pack $ "a@dev.washpost.com" ++ show x) (B8.pack "submit") (B8.pack "level1") 10 | x <- [1..]]

updateMetrics :: Int -> Handle ->IO ()
updateMetrics m h = do
    let q = UpdateMetricLevels (take m metrics)
    writeQuery h q

updateLimits :: Handle -> IO ()
updateLimits h = do
    let q = UpdateLimits [QLimit (B8.pack "level1") (B8.pack "submit") 2800]
    writeQuery h q

getOverLimit :: Handle -> IO ()
getOverLimit h = do
    writeQuery h GetOverLimit
    print =<< readReply h
    threadDelay 1000000

stop :: Handle -> IO ()
stop h = writeQuery h Stop

newConnHandle :: IO Handle
newConnHandle = do
    addrinfos <- getAddrInfo Nothing (Just "127.0.0.1") (Just "1813")
    let serveraddr = head addrinfos
    sock <- socket (addrFamily serveraddr) Stream defaultProtocol
    connect sock (addrAddress serveraddr)
    socketToHandle sock ReadWriteMode

myForkIO :: IO () -> IO (MVar ())
myForkIO io = do
    mvar <- newEmptyMVar
    forkFinally io (\_ -> putMVar mvar ())
    return mvar

