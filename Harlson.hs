module Harlson where

import Network.Socket

import Control.Applicative
import Control.Concurrent
import Control.DeepSeq
import Control.Exception
import Control.Monad

import Data.Time.Clock
import Data.Char
import qualified Data.List as L
import qualified Data.Map.Strict as Map
import qualified Data.ByteString.Lazy as B
import qualified Data.ByteString.Lazy.Char8 as B8

import System.IO

import Text.PrettyPrint

import Options
import Mavg
import Server
import Query

import TelnetHandler

data MData = MData { mdCounter :: !Int
                   } deriving Show

data OverLimit = OverLimit { oValue :: !Int
                           , oThrottle :: !Int
                           } deriving Show

data Metric = Metric { mMavg :: !(MVar Mavg)
                     , mData :: !(MVar MData)
                     }

data Key = Key { kKey      :: !B.ByteString
               , kEndpoint :: !B.ByteString
               } deriving (Show, Eq, Ord)

data LKey = LKey { lkLevel    :: !B.ByteString
                 , lkEndpoint :: !B.ByteString
                 } deriving (Show, Eq, Ord)

type MetricsMap = Map.Map Key Metric

type OverLimitsMap = Map.Map Key OverLimit

type LimitsMap = Map.Map LKey Int

type LevelsMap = Map.Map B.ByteString B.ByteString

data MavgAcc = MavgAcc { maAcc   :: !Int
                       , maMavgs :: ![Mavg]
                       } deriving Show

data Stats = Stats { statQueries :: !MavgAcc
                   , statMetrics :: !MavgAcc
                   } deriving Show

data YState = YState { sMetrics    :: !(MVar MetricsMap)
                     , sOverLimits :: !(MVar OverLimitsMap) -- the only modifier is mavgUpdater
                     , sLimits     :: !(MVar LimitsMap)
                     , sLevels     :: !(MVar LevelsMap)
                     , sStats      :: !(MVar Stats)
                     , sExit       :: !(MVar Int)
                     }

runHarlson :: Options -> IO ()
runHarlson opts = do
    mvMetrics <- newMVar Map.empty
    mvOverLimits <- newMVar Map.empty
    mvLimits <- newMVar Map.empty
    mvLevels <- newMVar Map.empty
    mvExit   <- newEmptyMVar
    mvStats  <- initialStats
    let ystate = YState mvMetrics mvOverLimits mvLimits mvLevels mvStats mvExit
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
    takeMVar mvExit
    return ()

waitPortClose :: MVar Int -> IO ()
waitPortClose mvExit = do
    hSetBuffering stdin NoBuffering
    try getChar :: IO (Either IOError Char)
    putMVar mvExit 0


force' :: (Show a) => a -> IO a
force' x = evaluate $ deepseq (show x) x

defaultLevel :: B.ByteString
defaultLevel = B8.pack "undefined"
defaultLimit :: Int
defaultLimit = 2000000000 

mavgUpdater :: YState -> IO ()
mavgUpdater ystate = do
    threadDelay 1000000
    metrics <- readMVar (sMetrics ystate)
    limits  <- readMVar (sLimits  ystate)
    levels  <- readMVar (sLevels  ystate)
    now     <- getCurrentTime
    let mvStats = sStats ystate
    bumpStats now mvStats
    modifyMVar_ mvStats force'
    let ms = Map.toList metrics
    forM_ ms $ \(k@(Key appkey ep), m) -> do
        mavg    <- readMVar (mMavg m)
        MData c <- swapMVar (mData m) (MData 0)
        let mavg' = bumpRate mavg now c
            ra    = rateAverage mavg'
            lvl   = Map.findWithDefault defaultLevel appkey levels
        modifyMVar_ (sOverLimits ystate) (\overLimits -> do
            let limit = Map.findWithDefault defaultLimit (LKey lvl ep) limits
            if ra > limit
                then do
                    let limit_l = fromIntegral limit :: Integer
                        ra_l    = fromIntegral ra :: Integer
                        p       = throttlePrecision * limit_l `div` ra_l
                        p_l     = fromIntegral p
                    return $! Map.insert k (OverLimit ra p_l) overLimits
                else return $! Map.delete k overLimits)
        swapMVar (mMavg m) $! mavg'
    mavgUpdater ystate

handler :: (Handle -> Query -> IO ()) -> SockAddr -> Handle -> IO ()
handler qp sa h = do
    isEof <- hIsEOF h
    if isEof
        then hClose h
        else readQuery h >>= qp h >> handler qp sa h

processQuery :: Options -> YState -> Handle -> Query -> IO ()
processQuery opts ystate h (UpdateMetricLevels qms) = do
    let qlevels  = map (\(QMetricLevel key _ level _) -> QLevel key level) qms
        qmetrics = map (\(QMetricLevel key endp _ count) -> QMetric key endp count) qms
    processQuery opts ystate h (UpdateLevels qlevels)
    processQuery opts ystate h (UpdateMetrics qmetrics)
processQuery _opts ystate _h (UpdateLevels newlevels) = do
    updateStats (sStats ystate) 1 0
    let toInsert = [(key, level) | QLevel key level <- newlevels]
    modifyMVar_ (sLevels ystate) $ \old -> 
        return $! L.foldl' (flip $ uncurry Map.insert) old toInsert
processQuery opts ystate _h (UpdateMetrics qms) = do
    updateStats (sStats ystate) 1 (length qms)
    let smooth = optSmoothing opts
        metrics = sMetrics ystate
    forM_ qms $ updateMetric smooth metrics
processQuery _opts ystate h GetOverLimit = do
    updateStats (sStats ystate) 1 0
    let mvOverLimits = sOverLimits ystate
    overLimitsMap <- readMVar mvOverLimits
    let os = Map.toList overLimitsMap
    writeReply h $ ReplyOverLimit [ROverLimit k e (OverLimitAdded val thr) | (Key k e, OverLimit val thr) <- os]
processQuery _opts ystate _h (UpdateLimits qls) = do
    updateStats (sStats ystate) 1 0
    let mvLimits = sLimits ystate
    modifyMVar_ mvLimits (\_oldLimits -> do
        let limits = Map.fromList [(LKey lvl ep, lim) | (QLimit lvl ep lim) <- qls]
        return limits)
processQuery _opts ystate h GetStats = do
    formattedStats <- prepareStats ystate
    writeReply h $ ReplyText formattedStats
processQuery _opts ystate _h Stop = putMVar (sExit ystate) 0
processQuery _opts _ystate h query =
    writeReply h $ ReplyText $ "Unknown query: " ++ show query

updateMetric :: Double -> MVar MetricsMap -> QMetric -> IO ()
updateMetric smoothingWindow mvMetrics (QMetric key ep cnt) = do
    let k = Key key ep
    metric <- modifyMVar mvMetrics $ \metrics ->
        case Map.lookup k metrics of
            Just m -> return (metrics, m)
            Nothing -> do
                m <- newMetric smoothingWindow
                return (Map.insert k m metrics, m)
    modifyMVar_ (mData metric) $ \(MData c) -> return $! MData (c + cnt)

newMetric :: Double -> IO Metric
newMetric smoothingWindow = do
    mavg <- mavgNewIO smoothingWindow
    ma <- newMVar mavg
    mc <- newMVar (MData 0)
    return $ Metric ma mc

statsWindows :: [(Double, String)]
statsWindows = [(60.0, "min"), (300.0, "5min"), (3600.0, "hour"), (86400.0, "day")]

initialStats :: IO (MVar Stats)
initialStats = do
    let mk = MavgAcc 0 <$> mapM (mavgNewIO . fst) statsWindows
    connects <- mk
    metrics  <- mk
    newMVar $ Stats connects metrics

updateStats :: MVar Stats -> Int -> Int -> IO ()
updateStats mvStats c m =
    modifyMVar_ mvStats (\(Stats conns mtrs) -> do
        let conns' = conns {maAcc = maAcc conns + c}
        let mtrs' = mtrs {maAcc = maAcc mtrs + m}
        return $ Stats conns' mtrs')

bumpStats :: UTCTime -> MVar Stats -> IO ()
bumpStats now mvStats = do
    let bump' n = map (\m -> bumpRate m now n)
    modifyMVar_ mvStats (\(Stats (MavgAcc c cs) (MavgAcc m ms)) -> do
        let cs' = bump' c cs
        let ms' = bump' m ms
        return $ Stats (MavgAcc 0 cs') (MavgAcc 0 ms'))


-- Telnet commands

runTelnetCmd :: YState -> String -> IO String
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
    levels <- readMVar $ sLevels ystate
    lims <- forM lims' $ \(Key key ep, Metric m _d) -> do
                    mavg <- readMVar m
                    let level = Map.findWithDefault defaultLevel key levels
                    return (key, ep, level, mavg)
    let doc = vcat [u key <> text "/" <> u ep <> text "/" <> u lev <> colon <+> int (rateAverage mavg)
                        | (key, ep, lev, mavg) <- lims]
    return $ render doc
runTelnetCmd _ystate "help" = return listTelnetCmds
runTelnetCmd _ystate "h" = return listTelnetCmds
runTelnetCmd _ystate _cmd =
    return "Unrecognized command"

listTelnetCmds :: String
listTelnetCmds = render $ vcat [text cmd <> text " -- " <> text desc | (cmd, desc) <- cmds]
    where cmds =
            [ ("s             ", "Show quick stats")
            , ("l             ", "Show limits")
            , ("showallmetrics", "List all metrics")
            , ("help (or h)   ", "Display help") ]


-- Preparing stats for Folsom/Riemann/Graphite

prepareStats :: YState -> IO String
prepareStats ystate = do
    metricStats <- prepareMetricStats ystate
    harlsonStats <- prepareHarlsonStats ystate
    return $ render $ metricStats $$ harlsonStats

prepareMetricStats :: YState -> IO Doc
prepareMetricStats ystate = do
    metrics <- Map.toList <$> readMVar (sMetrics ystate)
    docs <- mapM (\(Key k ep, Metric mvMavg _) -> do
            n <- rateAverage <$> readMVar mvMavg
            return $ constructMetricDoc k ep n
        ) metrics
    return $ vcat docs

prepareHarlsonStats :: YState -> IO Doc
prepareHarlsonStats ystate = do
    let get f = readMVar $ f ystate
    stats <- get sStats
    let (MavgAcc _ qs) = statQueries stats
    let (MavgAcc _ ms) = statMetrics stats

    let mkLineDoc name (v, k) = text "rls.stats." <> text name <> text "." <> text k <> semi <> int (rateAverage v)
    let mkSectionDoc (cs, name) = vcat $ map (mkLineDoc name) $ zip cs $ map snd statsWindows
    let doc1 = vcat $ map mkSectionDoc [(qs, "connects"), (ms, "metrics")]

    let sz x = Map.size <$> get x
    let mkCurrentDoc (vM, k) = do
        v <- vM
        let doc = text "rls.stats.current." <> text k <> semi <> int v
        return doc
    doc2 <- vcat <$> mapM mkCurrentDoc [ (sz sMetrics, "metrics")
                                       , (sz sLimits, "limits")
                                       , (sz sOverLimits, "overlimits")]
    return $ doc1 $$ doc2

constructMetricDoc :: B8.ByteString -> B8.ByteString -> Int -> Doc
constructMetricDoc k ep n =
    text "rls.metrics." <> u k <> text "." <> u ep <> semi <> int n where
        u = text . safeString . B8.unpack
        safeString = map r
        r c = if isAlphaNum c then c else '_'

