import Network.Socket
import Control.Monad
import Control.Concurrent
import Control.Exception
import System.IO
import qualified Data.ByteString.Lazy as B
import qualified Data.ByteString.Lazy.Char8 as B8

import Query

main :: IO ()
main = do
    let keysCount = 10000
    runJob 1 (updateMetrics keysCount)
    runJob 1 updateLimits
    runJob 1 (updateLevels keysCount)
    vars <- replicateM 300 $ myForkIO $ runJob 1 (updateMetrics 2000)
    mapM_ takeMVar vars
    print "Done put"
    runJob 10 getOverLimit
    runJob 1 stop

runJob :: Int -> (Handle -> IO ()) -> IO ()
runJob n job = bracket newConnHandle hClose (replicateM_ n . job)

keys :: [B.ByteString]
keys = [B8.pack $ "a@dev.washpost.com" ++ show x | x <- [1..] :: [Integer]]

endpoint :: B.ByteString
endpoint = B8.pack "submit"

levelname :: B.ByteString
levelname = B8.pack "level1"

metrics :: [QMetric]
metrics = [QMetric key endpoint 10 | key <- keys]

levels :: [QLevel]
levels = [QLevel key levelname | key <- keys]

updateMetrics :: Int -> Handle -> IO ()
updateMetrics m h = do
    let q = UpdateMetrics $ take m metrics
    writeQuery h q

updateLevels :: Int -> Handle -> IO ()
updateLevels m h = do
    let q = UpdateLevels $ take m levels
    writeQuery h q

updateLimits :: Handle -> IO ()
updateLimits h = do
    let q = UpdateLimits [QLimit levelname endpoint 2800]
    writeQuery h q

getOverLimit :: Handle -> IO ()
getOverLimit h = do
    writeQuery h GetOverLimit
    print . length . show =<< readReply h
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
    _ <- forkFinally io (\_ -> putMVar mvar ())
    return mvar

