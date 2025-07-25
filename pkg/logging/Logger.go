package logging

import (
	"log"
)

/*
Logging interface defines the basic functionalities of the Logger
*/
type Logging interface {
	Info(message string)
	Warning(message string)
	Error(message string)
}

/*
Logger struct object definition
*/
type Logger struct{}

/*
Info logs an information message and continues operation
*/
func (l *Logger) Info(message ...string) {
	output := "INFO: "
	for _, msg := range message {
		output += msg + " "
	}
	log.Println(output)
}

/*
Warning logs an error message and continues operation
*/
func (l *Logger) Warning(message string) {
	log.Println("WARNING: " + message)
}

/*
Error logs an error message and exits by raising fatal error
*/
func (l *Logger) Error(message string) {
	log.Fatalln("Error: " + message)
}

// EtlLogger creation and export of single logger message
var EtlLogger = Logger{}
