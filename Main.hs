module Main where

import Options
import Harlson

main = progOpts >>= runHarlson

