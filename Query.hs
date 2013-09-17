module Query where

import qualified Data.ByteString.Lazy as B
import qualified Data.ByteString.Lazy.Char8 as B8
import qualified Data.Binary.Put as Put
import qualified Data.Binary.Get as Get
import System.IO
import Debug.Trace
import Control.Applicative
import Control.Exception
import Control.Monad

data QMetric = QMetric { qmKey :: !B.ByteString
                       , qmEndpoint :: !B.ByteString
                       , qmLevel :: !B.ByteString
                       , qmCount :: !Int
                       } deriving Show

data QLimit = QLimit { qlLevel :: !B.ByteString
                     , qlEndpoint :: !B.ByteString
                     , qlLimit :: !Int
                     } deriving Show

data ROverLimit = ROverLimit { roKey :: !B.ByteString
                             , roEndpoint :: !B.ByteString
                             , roOverLimitChange :: !OverLimitChange
                             } deriving Show

data OverLimitChange = OverLimitAdded { roValue :: !Int
                                      , roThrottle :: !Int -- as a permille
                                      }
                     | OverLimitRemoved
                        deriving Show

data Query = UpdateMetrics ![QMetric]
           | UpdateLimits ![QLimit]
           | GetOverLimit
           | OverLimitUpdates
           | Stop
                deriving Show

data Reply = ReplyOverLimit ![ROverLimit]
           | ReplyOverLimitUpdate !ROverLimit
                deriving Show

readQuery :: Handle -> IO Query
readQuery h = do
    t <- B.hGet h 4
    readQueryByType h $ B8.unpack t

writeQuery :: Handle -> Query -> IO ()
writeQuery h (UpdateMetrics qmetrics) = do
    B.hPut h $ B8.pack "UPME"
    B.hPut h $ Put.runPut $ Put.putWord32be $ fromIntegral $ length qmetrics
    mapM_ (writeMetric h) qmetrics
    hFlush h
writeQuery h (UpdateLimits qlimits) = do
    B.hPut h $ B8.pack "UPLI"
    B.hPut h $ Put.runPut $ Put.putWord32be $ fromIntegral $ length qlimits
    mapM_ (writeLimit h) qlimits
    hFlush h
writeQuery h GetOverLimit = do
    B.hPut h $ B8.pack "GOVL"
    hFlush h
writeQuery h OverLimitUpdates = do
    B.hPut h $ B8.pack "OVLU"
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

readQueryByType :: Handle -> String -> IO Query
readQueryByType h "UPME" = do
    len <- readInt h
    fmap UpdateMetrics $ replicateM len (readMetric h)
readQueryByType h "UPLI" = do
    len <- readInt h
    fmap UpdateLimits $ replicateM len (readLimit h)
readQueryByType h "GOVL" = return GetOverLimit
readQueryByType h "OVLU" = return OverLimitUpdates
readQueryByType h "STOP" = return Stop

readReplyByType :: Handle -> String -> IO Reply
readReplyByType h "ROVL" = do
    len <- readInt h
    fmap ReplyOverLimit $ replicateM len (readOverLimit h)    
readReplyByType h "ROLU" = do
    fmap ReplyOverLimitUpdate $ readOverLimit h

writeMetric :: Handle -> QMetric -> IO ()
writeMetric h (QMetric key endpoint level count) = do
    writeShort h $ B.length key
    B.hPut h key
    writeShort h $ B.length endpoint
    B.hPut h endpoint
    writeShort h $ B.length level
    B.hPut h level
    writeInt h count

writeLimit :: Handle -> QLimit -> IO ()
writeLimit h (QLimit level endpoint limit) = do
    writeShort h $ B.length level
    B.hPut h level
    writeShort h $ B.length endpoint
    B.hPut h endpoint
    writeInt h limit

writeOverLimit :: Handle -> ROverLimit -> IO ()
writeOverLimit h (ROverLimit key endpoint change) = do
    writeShort h $ B.length key
    B.hPut h key
    writeShort h $ B.length endpoint
    B.hPut h endpoint
    case change of
        OverLimitAdded value throttle -> do
            writeShort h 0
            writeInt h value
            writeInt h throttle
        OverLimitRemoved ->
            writeShort h 1

readMetric :: Handle -> IO QMetric
readMetric h = do
    keyLen <- readShort h
    key <- B.hGet h keyLen
    epLen <- readShort h
    endpoint <- B.hGet h epLen
    lvlLen <- readShort h
    level <- B.hGet h lvlLen
    count <- readInt h
    return $ QMetric key endpoint level count

readLimit :: Handle -> IO QLimit
readLimit h = do
    lvlLen <- readShort h
    level <- B.hGet h lvlLen
    epLen <- readShort h
    endpoint <- B.hGet h epLen
    limit <- readInt h
    return $ QLimit level endpoint limit

readOverLimit :: Handle -> IO ROverLimit
readOverLimit h = do
    keyLen <- readShort h
    key <- B.hGet h keyLen
    epLen <- readShort h
    endpoint <- B.hGet h epLen
    changeType <- readShort h
    change <- case changeType of
                0 -> do
                    value <- readInt h
                    throttle <- readInt h
                    return $ OverLimitAdded value throttle
                1 -> return OverLimitRemoved
    return $ ROverLimit key endpoint change

readInt :: Handle -> IO Int
readInt h = do
    lenBin <- B.hGet h 4
    return $ fromIntegral $ Get.runGet Get.getWord32be lenBin

readShort :: Handle -> IO Int
readShort h = do
    lenBin <- B.hGet h 1
    return $ fromIntegral $ Get.runGet Get.getWord8 lenBin

writeInt :: (Integral a) => Handle -> a -> IO ()
writeInt h n = do
    let bin = Put.runPut $ Put.putWord32be $ fromIntegral n
    B.hPut h bin

writeShort :: (Integral a) => Handle -> a -> IO ()
writeShort h n = do
    let bin = Put.runPut $ Put.putWord8 $ fromIntegral n
    B.hPut h bin


---

bsToQueries :: B.ByteString -> [Query]
bsToQueries = Get.runGet (getQueries [])
 
getQueries :: [Query] -> Get.Get [Query]
getQueries acc = do
    isEmp <- Get.isEmpty
    if isEmp then return acc else 
        do
            q <- getQuery
            case q of
                Just query -> getQueries (query:acc)
                Nothing -> getQueries acc

getQuery :: Get.Get (Maybe Query)
getQuery = do
    code <- Get.getLazyByteString 4
    case B8.unpack code of
        "UPME" -> Just <$> UpdateMetrics <$> getMetrics
        "UPLI" -> Just <$> UpdateLimits <$> getLimits
        "GOVL" -> return $ Just GetOverLimit
        "OVLU" -> return $ Just OverLimitUpdates
        "STOP" -> return $ Just Stop
        "\0\0\0\0" -> 
                  return Nothing
        _      -> error ("Bad query type: " ++ show code)

getMetrics :: Get.Get [QMetric]
getMetrics = do
    length <- getWord32
    replicateM length getMetric

getMetric :: Get.Get QMetric
getMetric = QMetric <$> getStr <*> getStr <*> getStr <*> getWord32

getLimits :: Get.Get [QLimit]
getLimits = do
    length <- getWord32
    replicateM length getLimit

getLimit :: Get.Get QLimit
getLimit = QLimit <$> getStr <*> getStr <*> getWord32
-- Special

getWord8 :: Get.Get Int
getWord8 = fromIntegral <$> Get.getWord8

getWord32 :: Get.Get Int
getWord32 = fromIntegral <$> Get.getWord32be

getStr :: Get.Get B.ByteString
getStr = do 
    length <- getWord8
    Get.getLazyByteString $ fromIntegral length

--
putReply (ReplyOverLimit list) = do
    Put.putLazyByteString $ B8.pack "ROVL"
    putWord32 $ length list
    mapM putReplyOverLimit list
    return ()

putReplyOverLimit (ROverLimit key endpoint change) = do
    putBStr key
    putBStr endpoint
    case change of
        (OverLimitAdded value trottle) -> do
            putWord8 0
            putWord32 value
            putWord32 trottle
        OverLimitRemoved ->
            putWord8 1
    

putWord32 = Put.putWord32be . fromIntegral
putWord8  = Put.putWord8 . fromIntegral
putBStr bs = do
    putWord8 $ B.length bs
    Put.putLazyByteString bs

