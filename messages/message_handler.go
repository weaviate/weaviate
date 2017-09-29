/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 Weaviate. All rights reserved.
 * LICENSE: https://github.com/weaviate/weaviate/blob/master/LICENSE
 * AUTHOR: Bob van Luijt (bob@weaviate.com)
 * See www.weaviate.com for details
 * Contact: @weaviate_iot / yourfriends@weaviate.com
 */

package messages

import (
	"log"
	"os"
)

// ExitError exit the program and give standard weaviate-error message.
func ExitError(code int, message interface{}) {
	// Print Error
	ErrorMessage(message.(string))
	log.Println("ERROR: Exiting...")

	// Exit with code
	os.Exit(code)
}

// InfoMessage sends a message with 'INFO:' in front of it
func InfoMessage(message interface{}) {
	// Print Message
	log.Println("INFO: " + message.(string) + ".")
}

// ErrorMessage exit the program and give standard weaviate-error message.
func ErrorMessage(message interface{}) {
	// Print Error
	log.Println("ERROR: " + message.(string) + ". Needs to be resolved. For more info, check https://weaviate.com/.")
}
