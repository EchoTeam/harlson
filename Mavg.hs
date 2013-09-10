
module Mavg where

import Data.Time.Clock
import Control.Concurrent

data Mavg = Mavg
    { historicAvg :: Double
    , lastUpdateTS :: UTCTime
    , period :: Double
    , rateAverage :: Int
    }
    deriving (Eq, Ord)

instance Show Mavg where
    show m = show (historicAvg m, rateAverage m)

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
    m { historicAvg = (exp (-1.0 / p) * (h - ur) + ur) * exp ((1.0 - elapsed) / p)
      , lastUpdateTS = now
      , rateAverage = round (h * p)
      }
    where
        ur = realToFrac n
        h = historicAvg m
        p = period m
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
    1.0 <= realToFrac (diffUTCTime now (lastUpdateTS m))

test = do
    m0 <- mavgNewIO 6.0
    t0 <- getCurrentTime
    let m1 = bumpValue m0 t0 10
    threadDelay 10000000
    t1 <- getCurrentTime
    let m2 = bumpValue m1 t1 11
    print $ valueAverage m2
    print m2
