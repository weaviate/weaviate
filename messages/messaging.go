/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2018 Weaviate. All rights reserved.
 * LICENSE: https://github.com/creativesoftwarefdn/weaviate/blob/develop/LICENSE.md
 * AUTHOR: Bob van Luijt (bob@kub.design)
 * See www.creativesoftwarefdn.org for details
 * Contact: @CreativeSofwFdn / bob@kub.design
 */

package messages

import (
	"fmt"
	log "github.com/sirupsen/logrus"
	"os"
	"regexp"
	"runtime"
	"strings"
	"time"
)

// Messaging has some basic variables.
type Messaging struct {
	Debug bool
}

// ExitError exit the program and give standard weaviate-error message.
func (f *Messaging) ExitError(code int, message interface{}) {
	// Print Error
	f.ErrorMessage(message.(string))
	log.Errorln("ERROR: This error needs to be resolved. For more info, check creativesoftwarefdn.org/weaviate. Exiting now...")

	// Exit with code
	os.Exit(code)
}

// InfoMessage sends a message with 'INFO:' in front of it
func (f *Messaging) InfoMessage(message interface{}) {
	// Print Message
	log.Info(message)
}

// DebugMessage sends a message with 'DEBUG:' in front of it
func (f *Messaging) DebugMessage(message interface{}) {
	// Print Message
	if f.Debug {
		log.Debug(message)
	}
}

// ErrorMessage exit the program and give standard weaviate-error message.
func (f *Messaging) ErrorMessage(message interface{}) {
	// Print Error
	log.Error(message)
}

// TimeTrack tracks the time from execution to return of the function
// Usage: defer TimeTrack(time.Now())
func (f *Messaging) TimeTrack(start time.Time, info ...string) {
	elapsed := time.Since(start)

	// Skip this function, and fetch the PC and file for its parent
	pc, _, line, _ := runtime.Caller(1)

	// Retrieve a Function object this functions parent
	functionObject := runtime.FuncForPC(pc)

	// Regex to extract just the function name (and not the module path)
	extractFnName := regexp.MustCompile(`^.*\/(.*)$`)
	name := extractFnName.ReplaceAllString(functionObject.Name(), "$1")

	infoStr := ""
	if len(info) > 0 {
		infoStr = strings.Join(info, ", ") + ": "
	}

	f.DebugMessage(fmt.Sprintf("%s%s:%d took %s", infoStr, name, line, elapsed))
}
