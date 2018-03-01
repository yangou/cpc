package internal

import (
	"fmt"
	"log"
)

type Logger interface {
	Crit(format string, v ...interface{}) error
	Error(format string, v ...interface{}) error
	Warn(format string, v ...interface{}) error
	Info(format string, v ...interface{}) string
	Debug(format string, v ...interface{}) string
}

type logger struct{ enabled bool }

func (l *logger) Crit(format string, v ...interface{}) error {
	err := fmt.Errorf(format, v...)
	if l.enabled {
		log.Println(err.Error())
	}
	return err
}

func (l *logger) Error(format string, v ...interface{}) error {
	err := fmt.Errorf(format, v...)
	if l.enabled {
		log.Println(err.Error())
	}
	return err
}

func (l *logger) Warn(format string, v ...interface{}) error {
	err := fmt.Errorf(format, v...)
	if l.enabled {
		log.Println(err.Error())
	}
	return err
}

func (l *logger) Info(format string, v ...interface{}) string {
	msg := fmt.Sprintf(format, v...)
	if l.enabled {
		log.Println(msg)
	}
	return msg
}

func (l *logger) Debug(format string, v ...interface{}) string {
	msg := fmt.Sprintf(format, v...)
	if l.enabled {
		log.Println(msg)
	}
	return msg
}
