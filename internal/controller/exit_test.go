package controller

import "os"

// osExit is a thin wrapper so TestMain has a single exit point.
var osExit = os.Exit
