package diagnostic

import (
	"fmt"
	"io"
	"os"
	"time"
)

type LogLevel int16

const (
	LogLevel_Fatal   = 0
	LogLevel_Error   = iota
	LogLevel_Warning = iota
	LogLevel_Info    = iota
	LogLevel_Debug   = iota
	LogLevel_Trace   = iota

	LogLevel_Default = LogLevel_Debug
)

var (
	_LogLevelNames = map[LogLevel]string{
		LogLevel_Fatal:   "FATAL",
		LogLevel_Error:   "ERROR",
		LogLevel_Warning: "WARNING",
		LogLevel_Info:    "INFO",
		LogLevel_Debug:   "DEBUG",
		LogLevel_Trace:   "TRACE"}
	_LogLevel LogLevel = LogLevel_Default
)

//SetLogLevel set current log level. Only event with level >= the current are logged.
//Parameters:
// pLogLevel = log level to set
func SetLogLevel(pLogLevel LogLevel) error {

	if _LogLevelNames[pLogLevel] == "" {
		NewError("Invalid loglevel %d", nil, pLogLevel)
	}
	_LogLevel = pLogLevel
	return nil
}

//IsLogDebug return true if current loglevel accept debug
func IsLogDebug() bool {
	return _LogLevel >= LogLevel_Debug
}

//IsLogDebug return true if current loglevel accept trace
func IsLogTrace() bool {
	return _LogLevel >= LogLevel_Trace
}

func log(pLogLevel LogLevel, pModule string, pMessage string, pError error, pParameters ...interface{}) {

	vMessage := "[" + time.Now().String() + "]\t<" + _LogLevelNames[pLogLevel] + ">\t" + pModule + "\t"

	if len(pParameters) == 0 {
		vMessage += pMessage
	} else {
		vMessage += fmt.Sprintf(pMessage, pParameters)
	}
	vMessage += "\n"

	if pLogLevel > LogLevel_Debug {
		io.WriteString(os.Stderr, vMessage)
	} else {
		io.WriteString(os.Stdout, vMessage)
	}

	if pError != nil {
		io.WriteString(os.Stderr, pError.Error())
		io.WriteString(os.Stderr, "\n")
	}

	if pLogLevel == LogLevel_Fatal {
		os.Exit(10)
	}
}

//LogFatalIf in case the first parameter is not nil log a fatal error then terminate the process
//Parameters:
// pError = error to check (when nil doesn't perform anything, otherwise log it and exit with error)
// pModule = module description
// pMessage = log message to be formatted
// pParameters = optional parameters to format message
func LogFatalIfError(pError error, pModule string, pMessage string, pParameters ...interface{}) {

	if pError == nil {
		return
	}
	log(LogLevel_Fatal, pModule, pMessage, pError, pParameters...)
}

//LogError  log and message with severity error
//Parameters:
// pModule = module description
// pMessage = log message to be formatted
// pError = optional, cause
// pParameters = optional parameters to format message
func LogError(pModule string, pMessage string, pError error, pParameters ...interface{}) {
	log(LogLevel_Error, pModule, pMessage, pError, pParameters...)
}

//LogWarning log and message with severity warning
//Parameters:
// pModule = module description
// pMessage = log message to be formatted
// pError = optional, cause
// pParameters = optional parameters to format message
func LogWarning(pModule string, pMessage string, pError error, pParameters ...interface{}) {
	log(LogLevel_Warning, pModule, pMessage, pError, pParameters...)
}

//LogInfo log and message with severity info
//Parameters:
// pModule = module description
// pMessage = log message to be formatted
// pParameters = optional parameters to format message
func LogInfo(pModule string, pMessage string, pParameters ...interface{}) {
	log(LogLevel_Info, pModule, pMessage, nil, pParameters...)
}

//LogDebug log and message with severity debug
//Parameters:
// pModule = module description
// pMessage = log message to be formatted
// pParameters = optional parameters to format message
func LogDebug(pModule string, pMessage string, pParameters ...interface{}) {
	log(LogLevel_Debug, pModule, pMessage, nil, pParameters...)
}

//LogTrace log and message with severity trace
//Parameters:
// pModule = module description
// pMessage = log message to be formatted
// pParameters = optional parameters to format message
func LogTrace(pModule string, pMessage string, pParameters ...interface{}) {
	log(LogLevel_Trace, pModule, pMessage, nil, pParameters...)
}
