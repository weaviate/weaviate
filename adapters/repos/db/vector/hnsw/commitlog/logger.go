//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package commitlog

import (
	"bufio"
	"os"
)

type Logger struct {
	file *os.File
	bufw *bufio.Writer
}

func NewLogger(fileName string) *Logger {
	file, err := os.Create(fileName)
	if err != nil {
		panic(err)
	}

	return &Logger{file: file, bufw: bufio.NewWriterSize(file, 32*1024)}
}

func NewLoggerWithFile(file *os.File) *Logger {
	return &Logger{file: file, bufw: bufio.NewWriterSize(file, 32*1024)}
}

func (l *Logger) Write(p []byte) (n int, err error) {
	return l.bufw.Write(p)
}

func (l *Logger) FileSize() (int64, error) {
	i, err := l.file.Stat()
	if err != nil {
		return -1, err
	}

	return i.Size(), nil
}

func (l *Logger) FileName() (string, error) {
	i, err := l.file.Stat()
	if err != nil {
		return "", err
	}

	return i.Name(), nil
}

func (l *Logger) Flush() error {
	return l.bufw.Flush()
}

func (l *Logger) Close() error {
	if err := l.bufw.Flush(); err != nil {
		return err
	}

	if err := l.file.Close(); err != nil {
		return err
	}

	return nil
}
