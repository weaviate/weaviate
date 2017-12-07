/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 Weaviate. All rights reserved.
 * LICENSE: https://github.com/creativesoftwarefdn/weaviate/blob/develop/LICENSE.md
 * AUTHOR: Bob van Luijt (bob@weaviate.com)
 * See www.weaviate.com for details
 * Contact: @CreativeSofwFdn / yourfriends@weaviate.com
 */

package messages

import (
	"fmt"
	"log"
	"os"
)

// Messaging has some basic variables.
type Messaging struct {
	Debug bool
}

// ExitError exit the program and give standard weaviate-error message.
func (f *Messaging) ExitError(code int, message interface{}) {
	// Print Error
	f.ErrorMessage(message.(string))
	log.Println("ERROR: This error needs to be resolved. For more info, check https://weaviate.com/. Exiting now...")

	// Exit with code
	os.Exit(code)
}

// InfoMessage sends a message with 'INFO:' in front of it
func (f *Messaging) InfoMessage(message interface{}) {
	// Print Message
	log.Println("INFO: " + fmt.Sprint(message) + ".")
}

// DebugMessage sends a message with 'DEBUG:' in front of it
func (f *Messaging) DebugMessage(message interface{}) {
	// Print Message
	if f.Debug {
		log.Println("DEBUG: " + fmt.Sprint(message) + ".")
	}
}

// ErrorMessage exit the program and give standard weaviate-error message.
func (f *Messaging) ErrorMessage(message interface{}) {
	// Print Error
	log.Println("ERROR: " + fmt.Sprint(message) + ".")
}
