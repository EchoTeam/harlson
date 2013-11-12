module Harlson where

import Network.Socket
import Network.BSD

import Control.Exception
import Control.Applicative
import Control.Concurrent
import Control.Concurrent.MVar
import Control.DeepSeq
import Control.Exception

import Data.Time.Clock
import Data.Word
import Data.Maybe
import Data.Char
import qualified Data.Map.Strict as Map
import qualified Data.ByteString.Lazy as B
import qualified Data.ByteString.Lazy.Char8 as B8
import qualified Data.Binary.Put as Put
import qualified Data.Binary.Get as Get

import System.IO
import System.Exit

import Text.PrettyPrint

import Options
import Mavg
import Server
import Query

import TelnetHandler

data MData = MData { mdCounter :: !Int
                   , mdLevel :: !B.ByteString
                   } deriving Show

data OverLimit = OverLimit { oValue :: !Int
                           , oThrottle :: !Int
                           } deriving Show

data Metric = Metric { mMavg :: !(MVar Mavg)
                     , mData :: !(MVar MData)
                     }

data Key = Key { kKey :: !B.ByteString
               , kEndpoint :: !B.ByteString
               } deriving (Show, Eq, Ord)

data LKey = LKey { lkLevel :: !B.ByteString
                 , lkEndpoint :: !B.ByteString
                 } deriving (Show, Eq, Ord)

type MetricsMap = Map.Map Key Metric

type OverLimitsMap = Map.Map Key OverLimit

type LimitsMap = Map.Map LKey Int

data MavgAcc = MavgAcc { maAcc :: !Int
                       , maMavgs :: ![Mavg]
                       } deriving Show

data Stats = Stats { statQueries :: !MavgAcc
                   , statMetrics :: !MavgAcc
                   } deriving Show

data YState = YState { sMetrics :: !(MVar MetricsMap)
                     , sOverLimits :: !(MVar OverLimitsMap) -- the only modifier is mavgUpdater
                     , sLimits :: !(MVar LimitsMap)
                     , sStats :: !(MVar Stats)
                     , sExit :: !(MVar Int)
                     }

runHarlson opts = do
    mvMetrics <- newMVar Map.empty
    mvOverLimits <- newMVar Map.empty
    mvLimits <- newMVar Map.empty
    mvExit <- newEmptyMVar
    mvStats <- initialStats
    let ystate = YState mvMetrics mvOverLimits mvLimits mvStats mvExit
    let queryProcessor = processQuery opts ystate
    forkIO $ mavgUpdater ystate
    forkIO $ serveTCP (optPort opts) (handler queryProcessor)
    forkIO $ serveTCP (optTelnetPort opts) $ handleTelnet $ runTelnetCmd ystate
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


force' :: (Show a) => a -> IO ()
force' = evaluate . flip deepseq () . show

mavgUpdater :: YState -> IO ()
mavgUpdater ystate = do
    threadDelay 1000000
    metrics <- readMVar (sMetrics ystate)
    limits <- readMVar (sLimits ystate)
    now <- getCurrentTime
    bumpStats now (sStats ystate)
    force' =<< runTelnetCmd ystate "s"
    let ms = Map.toList metrics
    mapM_ (\(k@(Key _ ep), m)  -> do
        mavg <- readMVar (mMavg m)
        (c, lvl) <- modifyMVar (mData m) (\(MData c lvl) -> return (MData 0 lvl, (c, lvl)))
        let mavg' = bumpRate mavg now c
        let ra = rateAverage mavg'
        modifyMVar_ (sOverLimits ystate) (\overLimits -> do
            let limit = Map.findWithDefault 2000000000 (LKey lvl ep) limits
            if ra > limit
                then do
                    let limit_l = fromIntegral limit :: Integer
                    let ra_l = fromIntegral ra :: Integer
                    let p = fromIntegral (1000000 * limit_l `div` ra_l)
                    return $! Map.insert k (OverLimit ra p) overLimits
                else return $! Map.delete k overLimits)
        swapMVar (mMavg m) $! mavg') ms
    mavgUpdater ystate

handler :: (Handle -> Query -> IO ()) -> SockAddr -> Handle -> IO ()
handler qp sa h = do
    isEof <- hIsEOF h
    if isEof
        then hClose h
        else readQuery h >>= qp h >> handler qp sa h

processQuery :: Options -> YState -> Handle -> Query -> IO ()
processQuery opts ystate h (UpdateMetrics qms) = do
    updateStats (sStats ystate) 1 (length qms)
    mapM_ (updateMetric (optSmoothing opts) (sMetrics ystate) h) qms
processQuery opts ystate h GetOverLimit = do
    updateStats (sStats ystate) 1 0
    let mvOverLimits = sOverLimits ystate
    overLimitsMap <- readMVar mvOverLimits
    let os = Map.toList overLimitsMap
    writeReply h $ ReplyOverLimit [ROverLimit k e (OverLimitAdded val thr) | (Key k e, OverLimit val thr) <- os]
processQuery opts ystate h (UpdateLimits qls) = do
    updateStats (sStats ystate) 1 0
    let mvLimits = sLimits ystate
    modifyMVar_ mvLimits (\limits -> do
        let limits' = Map.fromList [(LKey lvl ep, lim) | (QLimit lvl ep lim) <- qls]
        return limits')
processQuery opts ystate h GetStats = do
    formattedStats <- prepareStats opts ystate
    writeReply h $ ReplyText formattedStats
processQuery opts ystate h Stop = putMVar (sExit ystate) 0

updateMetric :: Double -> MVar MetricsMap -> Handle -> QMetric -> IO ()
updateMetric smoothingWindow mvMetrics h (QMetric key ep lvl cnt) = do
    let k = Key key ep
    metric <- modifyMVar mvMetrics (\metrics ->
        case Map.lookup k metrics of
            Just m -> return (metrics, m)
            Nothing -> do
                m <- newMetric smoothingWindow
                return (Map.insert k m metrics, m))
    modifyMVar_ (mData metric) (\(MData c _) -> return $! MData (c + cnt) lvl)

newMetric :: Double -> IO Metric
newMetric smoothingWindow = do
    mavg <- mavgNewIO smoothingWindow
    ma <- newMVar mavg
    mc <- newMVar (MData 0 B.empty)
    return $ Metric ma mc

statsWindows = [(60.0, "min"), (300.0, "5min"), (3600.0, "hour"), (86400.0, "day")]

initialStats :: IO (MVar Stats)
initialStats = do
    let mk = MavgAcc 0 <$> (mapM mavgNewIO $ map fst statsWindows)
    connects <- mk
    metrics <- mk
    newMVar $ Stats connects metrics

updateStats :: MVar Stats -> Int -> Int -> IO ()
updateStats mvStats c m =
    modifyMVar_ mvStats (\(Stats conns mtrs) -> do
        let conns' = conns {maAcc = maAcc conns + c}
        let mtrs' = mtrs {maAcc = maAcc mtrs + m}
        return $ Stats conns' mtrs')

bumpStats now mvStats = do
    let bump n = map (\m -> bumpRate m now n)
    modifyMVar_ mvStats (\(Stats (MavgAcc c cs) (MavgAcc m ms)) -> do
        let cs' = bump c cs
        let ms' = bump m ms
        return $ Stats (MavgAcc 0 cs') (MavgAcc 0 ms'))


-- Telnet commands

runTelnetCmd ystate "s" = do
    let get f = readMVar $ f ystate
    (Stats (MavgAcc _ qs) (MavgAcc _ ms)) <- get sStats
    let ds = [(nm, zip (map rateAverage ls) (map snd statsWindows))
                | (nm, ls) <- [("Queries", qs), ("Metrics", ms)]]
    let statsDoc = vcat [text nm <> colon <+> nest 4
                            (vcat [int v <+> text "per" <+> text s
                                | (v, s) <- ks])
                            | (nm, ks) <- ds]
    mesize <- Map.size <$> get sMetrics
    lisize <- Map.size <$> get sLimits
    olsize <- Map.size <$> get sOverLimits
    let lenDoc = text "Metrics:" <+> int mesize $$ text "Limits:" <+> int lisize $$ text "Over:" <+> int olsize 
    return $ render (statsDoc $$ lenDoc)
runTelnetCmd ystate "l" = do
    let u = text . B8.unpack
    lims <- Map.toAscList <$> readMVar (sLimits ystate)
    let doc = vcat [u lev <> text "/" <> u ep <> colon <+> int v | (LKey lev ep, v) <- lims]
    return $ render doc
runTelnetCmd ystate "showallmetrics" = do
    let u = text . B8.unpack
    lims' <- Map.toAscList <$> readMVar (sMetrics ystate)
    lims <- mapM (\(Key key ep, Metric m d) -> do
                    mavg <- readMVar m
                    MData _ lev <- readMVar d
                    return (key, ep, lev, mavg)) lims'
    let doc = vcat [u key <> text "/" <> u ep <> text "/" <> u lev <> colon <+> int (rateAverage mavg)
                        | (key, ep, lev, mavg) <- lims]
    return $ render doc
runTelnetCmd ystate "help" = return listTelnetCmds
runTelnetCmd ystate "h" = return listTelnetCmds
runTelnetCmd ystate cmd =
    return "Unrecognized command"

listTelnetCmds = render $ vcat [text cmd <> text " -- " <> text desc | (cmd, desc) <- cmds]
    where cmds =
            [ ("s             ", "Show quick stats")
            , ("l             ", "Show limits")
            , ("showallmetrics", "List all metrics")
            , ("help (or h)   ", "Display help") ]


-- Preparing stats for Folsom/Riemann/Graphite

prepareStats :: Options -> YState -> IO String
prepareStats opts ystate = do
    metricStats <- prepareMetricStats opts ystate
    harlsonStats <- prepareHarlsonStats opts ystate
    return $ render $ metricStats $$ harlsonStats

prepareMetricStats opts ystate = do
    metrics <- Map.toList <$> readMVar (sMetrics ystate)
    docs <- mapM (\(key@(Key k ep), Metric mvMavg _) -> do
            n <- rateAverage <$> readMVar mvMavg
            return $ constructMetricDoc k ep n
        ) metrics
    return $ vcat docs

prepareHarlsonStats opts ystate = do
    let get f = readMVar $ f ystate
    stats <- get sStats
    let (MavgAcc _ qs) = statQueries stats
    let (MavgAcc _ ms) = statMetrics stats

    let mkLineDoc name (v, k) = text "rls.stats." <> text name <> text "." <> text k <> semi <> int (rateAverage v)
    let mkSectionDoc (cs, name) = vcat $ map (mkLineDoc name) $ zip cs $ map snd statsWindows
    let doc1 = vcat $ map mkSectionDoc [(qs, "connects"), (ms, "metrics")]

    mesize <- Map.size <$> get sMetrics
    lisize <- Map.size <$> get sLimits
    olsize <- Map.size <$> get sOverLimits

    let sz x = Map.size <$> get x
    let mkCurrentDoc (vM, k) = do
        v <- vM
        let doc = text "rls.stats.current." <> text k <> semi <> int v
        return doc
    doc2 <- vcat <$> mapM mkCurrentDoc [ (sz sMetrics, "metrics")
                                       , (sz sLimits, "limits")
                                       , (sz sOverLimits, "overlimits")]
    return $ doc1 $$ doc2

constructMetricDoc k ep n =
    text "rls.metrics." <> u k <> text "." <> u ep <> semi <> int n where
        u = text . safeString . B8.unpack
        safeString = map r
        r c = if isAlphaNum c then c else '_'

