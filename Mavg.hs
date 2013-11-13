
module Mavg where

import Data.Time.Clock
import Control.Concurrent

data Mavg = Mavg
    { historicAvg :: !Double
    , lastUpdateTS :: !UTCTime
    , period :: !Double
    , rateAverage :: !Int
    }
    deriving (Eq, Ord)

instance Show Mavg where
    show m = show (historicAvg m, rateAverage m)

magicFactor :: Double
magicFactor = 0.22

mavgNew :: Double -> UTCTime -> Mavg
mavgNew smoothing_window now =
    Mavg 
        { historicAvg = 0.0
        , period = smoothing_window
        , lastUpdateTS = now
        , rateAverage = 0
        }

mavgNewIO :: Double -> IO Mavg
mavgNewIO smoothing_window = do
    ts <- getCurrentTime 
    return Mavg 
        { historicAvg = 0.0
        , period = smoothing_window
        , lastUpdateTS = ts
        , rateAverage = 0
        }

bumpRate :: Mavg -> UTCTime -> Int -> Mavg 
bumpRate m now n =
    m { historicAvg = h'
      , lastUpdateTS = now
      , rateAverage = round (h' * p)
      }
    where
        ur = realToFrac n
        h = historicAvg m
        p = period m
        h' = (exp (-1.0 / p / magicFactor) * (h - ur) + ur) * exp ((1.0 - elapsed) / p / magicFactor)
        elapsed = realToFrac $ diffUTCTime now (lastUpdateTS m)

bumpValue :: Mavg -> UTCTime -> Double -> Mavg
bumpValue m now value =
    m { historicAvg = (a * value) + (e * h)
      , lastUpdateTS = now
      }
    where
        h = historicAvg m
        p = period m
        elapsed = realToFrac $ diffUTCTime now (lastUpdateTS m)
        e = exp (-elapsed / p)
        a = 1.0 - e

valueAverage :: Mavg -> Double
valueAverage = historicAvg

needsUpdate :: Mavg -> UTCTime -> Bool
needsUpdate m now =
    1.0 <= (realToFrac (diffUTCTime now (lastUpdateTS m)) :: Double)

test :: IO ()
test = do
    m0 <- mavgNewIO 6.0
    t0 <- getCurrentTime
    let m1 = bumpValue m0 t0 10
    threadDelay 10000000
    t1 <- getCurrentTime
    let m2 = bumpValue m1 t1 11
    print $ valueAverage m2
    print m2

test2 :: IO ()
test2 = do
    m <- mavgNewIO 60.0
    bump 600 m 1 

bump :: Int -> Mavg -> Int -> IO ()
bump 0 _ _ = return ()
bump nc m n = do
    threadDelay 200000
    t <- getCurrentTime
    print (realToFrac $ diffUTCTime t (lastUpdateTS m) :: Double)
    print $ period m
    print $ exp (-1.0 / period m)
    let m' = bumpRate m t n
    print m'
    bump (nc - 1) m' n

