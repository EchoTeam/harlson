module Main where

import Options
import Harlson

main :: IO ()
main = progOpts >>= runHarlson

