module Harlson where

import Network.Socket
import Network.BSD

import Control.Exception
import Control.Monad
import Control.Applicative
import Control.Concurrent
import Control.Concurrent.MVar

import Data.Time.Clock
import Data.Word
import Data.Maybe
import qualified Network.Socket.ByteString.Lazy as NB
import qualified Data.Map.Strict as Map
import qualified Data.ByteString.Lazy as B
import qualified Data.Binary.Put as Put
import qualified Data.Binary.Get as Get

import System.IO
import System.Exit

import Options
import Mavg
import Server
import Query

data MData = MData { mdCounter :: Int
                   , mdLevel :: B.ByteString
                   } deriving Show

data OverLimit = OverLimit { oValue :: Int
                           , oThrottle :: Int
                           } deriving Show

data Metric = Metric { mMavg :: MVar Mavg
                     , mData :: MVar MData
                     }

data Key = Key { kKey :: B.ByteString
               , kEndpoint :: B.ByteString
               } deriving (Show, Eq, Ord)

data LKey = LKey { lkLevel :: B.ByteString
                 , lkEndpoint :: B.ByteString
                 } deriving (Show, Eq, Ord)

type MetricsMap = Map.Map Key Metric

type OverLimitsMap = Map.Map Key OverLimit

type LimitsMap = Map.Map LKey Int

data YState = YState { sMetrics :: MVar MetricsMap
                     , sOverLimits :: MVar OverLimitsMap -- the only modifier is mavgUpdater
                     , sLimits :: MVar LimitsMap
                     , sExit :: MVar Int
                     }

runHarlson opts = do
    mvMetrics <- newMVar Map.empty
    mvOverLimits <- newMVar Map.empty
    mvLimits <- newMVar Map.empty
    mvExit <- newEmptyMVar
    let ystate = YState mvMetrics mvOverLimits mvLimits mvExit
    let queryProcessor = processQuery ystate
    forkIO $ mavgUpdater ystate
    forkIO $ serveTCP (optPort opts) (handler queryProcessor)
    case optMode opts of
        ErlangPortMode -> do
            forkIO $ waitPortClose mvExit
            return ()
        StandaloneMode ->
            return ()
    exitCode <- takeMVar mvExit
    return ()

waitPortClose mvExit = do
    hSetBuffering stdin NoBuffering
    r <- try getChar :: IO (Either IOError Char)
    putMVar mvExit 0

mavgUpdater :: YState -> IO ()
mavgUpdater ystate = do
    threadDelay 1000000
    metrics <- readMVar (sMetrics ystate)
    limits <- readMVar (sLimits ystate)
    now <- getCurrentTime
    let ms = Map.toList metrics
    mapM_ (\(k@(Key _ ep), m)  -> do
        mavg <- readMVar (mMavg m)
        (c, lvl) <- modifyMVar (mData m) (\(MData c lvl) -> return $! (MData 0 lvl, (c, lvl)))
        let mavg' = bumpRate mavg now c
        let ra = rateAverage mavg'
        modifyMVar_ (sOverLimits ystate) (\overLimits -> do
            let limit = (Map.findWithDefault 2000000000 (LKey lvl ep) limits)
            if ra > limit
                then return $! Map.insert k (OverLimit ra (1000 * limit `div` ra)) overLimits
                else return $! Map.delete k overLimits)
        swapMVar (mMavg m) $! mavg') ms
    mavgUpdater ystate

handler :: (Query -> IO (Put.Put)) -> SockAddr -> Socket -> IO ()
handler qp sa sock = do
    binary <- NB.getContents sock :: IO B.ByteString
    -- puts <- mapM qp $ bsToQueries binary
    -- let answer = Put.runPut $ sequence_ puts
    -- NB.send sock answer
    forM_ (bsToQueries binary) $ \query -> do
        answer <- qp query        
        NB.send sock $ Put.runPut answer
    return ()

processQuery :: YState -> Query -> IO (Put.Put)
processQuery ystate (UpdateMetrics qms) = do
    mapM_ (updateMetric (sMetrics ystate)) qms
    return Put.flush
processQuery ystate GetOverLimit = do
    let mvOverLimits = sOverLimits ystate
    overLimitsMap <- readMVar mvOverLimits
    let os  = Map.toList overLimitsMap
    let ans = ReplyOverLimit [ROverLimit k e (OverLimitAdded val thr) | (Key k e, OverLimit val thr) <- os]
    return $ putReply ans
processQuery ystate (UpdateLimits qls) = do
    let mvLimits = sLimits ystate
    modifyMVar_ mvLimits (\limits -> do
        let limits' = Map.fromList [(LKey lvl ep, lim) | (QLimit lvl ep lim) <- qls]
        return limits')
    return Put.flush
processQuery ystate Stop = do
    putMVar (sExit ystate) 0
    return Put.flush

updateMetric :: MVar MetricsMap -> QMetric -> IO ()
updateMetric mvMetrics (QMetric key ep lvl cnt) = do
    let k = Key key ep
    metric <- modifyMVar mvMetrics (\metrics ->
        case Map.lookup k metrics of
            Just m -> return $! (metrics, m)
            Nothing -> do
                m <- newMetric
                return $! (Map.insert k m metrics, m))
    modifyMVar_ (mData metric) (\(MData c _) -> return $! MData (c + cnt) lvl)

newMetric :: IO Metric
newMetric = do
    mavg <- mavgNewIO 60.0
    ma <- newMVar mavg
    mc <- newMVar (MData 0 B.empty)
    return $ Metric ma mc
