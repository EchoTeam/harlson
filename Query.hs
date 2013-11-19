module Query where

import qualified Data.ByteString.Lazy as B
import qualified Data.ByteString.Lazy.Char8 as B8
import qualified Data.Binary.Put as Put
import qualified Data.Binary.Get as Get
import System.IO
import Control.Monad
import Control.Applicative

-- BC
data QMetricLevel = QMetricLevel { qmlKey   :: !B.ByteString
                                 , qmlEndpoint :: !B.ByteString
                                 , qmlLevel :: !B.ByteString
                                 , qmlCount :: !Int
                                 } deriving Show
-- /BC

data QMetric = QMetric { qmKey     :: !B.ByteString
                       , qmEndoint :: !B.ByteString
                       , qmCount   :: !Int
                       } deriving Show

data QLimit = QLimit { qlLevel    :: !B.ByteString
                     , qlEndpoint :: !B.ByteString
                     , qlLimit    :: !Int
                     } deriving Show

data QLevel = QLevel { qaKey   :: !B.ByteString
                     , qaLevel :: !B.ByteString
                     } deriving Show

data ROverLimit = ROverLimit { roKey      :: !B.ByteString
                             , roEndpoint :: !B.ByteString
                             , roOverLimitChange :: !OverLimitChange
                             } deriving Show

data OverLimitChange = OverLimitAdded { roValue    :: !Int
                                      , roThrottle :: !Int -- as a permille
                                      }
                     | OverLimitRemoved
                        deriving Show

data Query = UpdateMetrics ![QMetric]
           | UpdateLimits  ![QLimit]
           | UpdateLevels  ![QLevel]
           | UpdateMetricLevels ![QMetricLevel]
           | GetOverLimit
           | OverLimitUpdates
           | GetStats
           | UnknownQuery
           | Stop
                deriving Show

data Reply = ReplyOverLimit ![ROverLimit]
           | ReplyOverLimitUpdate !ROverLimit
           | ReplyText String
           | ReplyUnknown
                deriving Show

throttlePrecision :: Integer
throttlePrecision = 1000000 

readQuery :: Handle -> IO Query
readQuery h = do
    t <- B.hGet h 4
    readQueryByType h $ B8.unpack t

writeQuery :: Handle -> Query -> IO ()
-- BC
writeQuery h (UpdateMetricLevels qmetrics) = do
    B.hPut h $ B8.pack "UPME"
    B.hPut h $ Put.runPut $ Put.putWord32be $ fromIntegral $ length qmetrics
    mapM_ (writeMetricLevel h) qmetrics
    hFlush h
-- /BC
writeQuery h (UpdateMetrics qmetrics) = do
    B.hPut   h $ B8.pack "UPMT"
    writeInt h $ length qmetrics
    mapM_ (writeMetric h) qmetrics
    hFlush h
writeQuery h (UpdateLimits qlimits) = do
    B.hPut   h $ B8.pack "UPLI"
    writeInt h $ length qlimits
    mapM_ (writeLimit h) qlimits
    hFlush h
writeQuery h (UpdateLevels qlevels) = do
    B.hPut   h $ B8.pack "UPLE"
    writeInt h $ length qlevels
    mapM_ (writeLevel h) qlevels
    hFlush h
writeQuery h GetOverLimit = do
    B.hPut h $ B8.pack "GOVL"
    hFlush h
writeQuery h OverLimitUpdates = do
    B.hPut h $ B8.pack "OVLU"
    hFlush h
writeQuery h GetStats = do
    B.hPut h $ B8.pack "STAT"
    hFlush h
writeQuery h UnknownQuery = do
    B.hPut h $ B8.pack "UNKN"
    hFlush h
writeQuery h Stop = do
    B.hPut h $ B8.pack "STOP"
    hFlush h

readReply :: Handle -> IO Reply
readReply h = do
    t <- B.hGet h 4
    readReplyByType h $ B8.unpack t
    
writeReply :: Handle -> Reply -> IO ()
writeReply h (ReplyOverLimit overlimits) = do
    B.hPut h $ B8.pack "ROVL"
    writeInt h $ length overlimits
    mapM_ (writeOverLimit h) overlimits
    hFlush h
writeReply h (ReplyOverLimitUpdate overlimit) = do
    B.hPut h $ B8.pack "ROLU"
    writeOverLimit h overlimit
    hFlush h
writeReply h (ReplyText text) = do
    B.hPut h $ B8.pack "RTEX"
    let binText = B8.pack text
    let len = B8.length binText
    writeInt h len
    B.hPut h binText
    hFlush h
writeReply h ReplyUnknown = do
    B.hPut h $ B8.pack "RUNK"
    hFlush h

readQueryByType :: Handle -> String -> IO Query
-- BC
readQueryByType h "UPME" = do
    len <- readInt h
    UpdateMetricLevels <$> replicateM len (readMetricLevel h)
-- /BC
readQueryByType h "UPLI" = do
    len <- readInt h
    UpdateLimits <$> replicateM len (readLimit h)
readQueryByType h "UPLE" = do
    len <- readInt h
    UpdateLevels <$> replicateM len (readLevel h)
readQueryByType h "UPMT" = do
    len <- readInt h
    UpdateMetrics <$> replicateM len (readMetric h)
readQueryByType _h "GOVL" = return GetOverLimit
readQueryByType _h "OVLU" = return OverLimitUpdates
readQueryByType _h "STAT" = return GetStats
readQueryByType _h "STOP" = return Stop
readQueryByType _h _ = return UnknownQuery

readReplyByType :: Handle -> String -> IO Reply
readReplyByType h "ROVL" = do
    len <- readInt h
    ReplyOverLimit <$> replicateM len (readOverLimit h)    
readReplyByType h "ROLU" =
    ReplyOverLimitUpdate <$> readOverLimit h
readReplyByType h "RTEX" = do
    len <- readInt h
    binText <- B.hGet h len
    return $ ReplyText $ B8.unpack binText
readReplyByType _h _ = return ReplyUnknown

writeMetricLevel :: Handle -> QMetricLevel -> IO ()
writeMetricLevel h (QMetricLevel key endpoint level count) = do
    writeString h key
    writeString h endpoint
    writeString h level
    writeInt    h count

writeMetric :: Handle -> QMetric -> IO ()
writeMetric h (QMetric key endpoint count) = do
    writeString h key
    writeString h endpoint
    writeInt    h count

writeLimit :: Handle -> QLimit -> IO ()
writeLimit h (QLimit level endpoint limit) = do
    writeString h level
    writeString h endpoint
    writeInt    h limit

writeLevel :: Handle -> QLevel -> IO ()
writeLevel h (QLevel key level) = do
    writeString h key
    writeString h level

writeOverLimit :: Handle -> ROverLimit -> IO ()
writeOverLimit h (ROverLimit key endpoint change) = do
    writeString h key
    writeString h endpoint
    case change of
        OverLimitAdded value throttle -> do
            writeShort h (0 :: Int)
            writeInt h value
            writeInt h throttle
        OverLimitRemoved ->
            writeShort h (1 :: Int)

readMetricLevel :: Handle -> IO QMetricLevel
readMetricLevel h = 
    QMetricLevel <$> readString h <*> readString h <*> readString h <*> readInt h

readLimit :: Handle -> IO QLimit
readLimit h =
    QLimit <$> readString h <*> readString h <*> readInt h

readOverLimit :: Handle -> IO ROverLimit
readOverLimit h = do
    overlimit <- ROverLimit <$> readString h <*> readString h
    changeType <- readShort h
    change <- case changeType of
                0 -> do
                    value <- readInt h
                    throttle <- readInt h
                    return $ OverLimitAdded value throttle
                _ -> return OverLimitRemoved
    return $! overlimit change

readMetric :: Handle -> IO QMetric
readMetric h = 
    QMetric <$> readString h <*> readString h <*> readInt h

readLevel :: Handle -> IO QLevel
readLevel h =
    QLevel <$> readString h <*> readString h

readInt :: Handle -> IO Int
readInt h = do
    lenBin <- B.hGet h 4
    return $ fromIntegral $ Get.runGet Get.getWord32be lenBin

readShort :: Handle -> IO Int
readShort h = do
    lenBin <- B.hGet h 1
    return $ fromIntegral $ Get.runGet Get.getWord8 lenBin

readString :: Handle -> IO B.ByteString
readString h = readShort h >>= B.hGet h

writeInt :: (Integral a) => Handle -> a -> IO ()
writeInt h n = do
    let bin = Put.runPut $ Put.putWord32be $ fromIntegral n
    B.hPut h bin

writeShort :: (Integral a) => Handle -> a -> IO ()
writeShort h n = do
    let bin = Put.runPut $ Put.putWord8 $ fromIntegral n
    B.hPut h bin

writeString :: Handle -> B.ByteString -> IO ()
writeString h s = do
    writeShort h $ B.length s
    B.hPut h s

