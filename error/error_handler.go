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

package errorHandler

import (
	"log"
	"os"
)

// ExitError exit the program and give standard weaviate-error message.
func ExitError(code int, message string) {
	// Print Error
	log.Println("ERROR: " + message + ". Needs to be resolved. For more info, check https://weaviate.com/.")
	log.Println("ERROR: Exiting...")

	// Exit with code
	os.Exit(code)
}
