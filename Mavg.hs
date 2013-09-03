
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

mavg_new :: Double -> UTCTime -> Mavg
mavg_new smoothing_window now = do
    Mavg 
        { historicAvg = 0.0
        , period = smoothing_window
        , lastUpdateTS = now
        , rateAverage = 0
        }

mavg_new_io :: Double -> IO Mavg
mavg_new_io smoothing_window = do
    ts <- getCurrentTime 
    return $ Mavg 
        { historicAvg = 0.0
        , period = smoothing_window
        , lastUpdateTS = ts
        , rateAverage = 0
        }

bump_rate :: Mavg -> UTCTime -> Int -> Mavg 
bump_rate m now n =
    m { historicAvg = ((exp (-1.0 / p)) * (h - ur) + ur) * (exp ((1.0 - elapsed) / p))
      , lastUpdateTS = now
      , rateAverage = round (h * p)
      }
    where
        ur = realToFrac n
        h = historicAvg m
        p = period m
        elapsed = realToFrac $ diffUTCTime now (lastUpdateTS m)

bump_value :: Mavg -> UTCTime -> Double -> Mavg
bump_value m now value =
    m { historicAvg = (a * value) + (e * h)
      , lastUpdateTS = now
      }
    where
        h = historicAvg m
        p = period m
        elapsed = realToFrac $ diffUTCTime now (lastUpdateTS m)
        e = exp (-elapsed / p)
        a = 1.0 - e

value_average :: Mavg -> Double
value_average = historicAvg

rate_average :: Mavg -> Int
rate_average = rateAverage

needs_update :: Mavg -> UTCTime -> Bool
needs_update m now =
    1.0 <= (realToFrac $ diffUTCTime now (lastUpdateTS m))

test = do
    m0 <- mavg_new_io 6.0
    t0 <- getCurrentTime
    let m1 = bump_value m0 t0 10
    threadDelay 10000000
    t1 <- getCurrentTime
    let m2 = bump_value m1 t1 11
    print $ value_average m2
    print m2
