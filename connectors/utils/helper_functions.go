/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 Weaviate. All rights reserved.
 * LICENSE: https://github.com/weaviate/weaviate/blob/develop/LICENSE.md
 * AUTHOR: Bob van Luijt (bob@weaviate.com)
 * See www.weaviate.com for details
 * Contact: @weaviate_iot / yourfriends@weaviate.com
 */

package connutils

import (
	"errors"
	"fmt"
	"regexp"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/weaviate/weaviate/models"

	"log"
	"net"

	gouuid "github.com/satori/go.uuid"
)

// NewDatabaseObjectFromPrincipal creates a new object with default values, out of principle object
// func NewDatabaseObjectFromPrincipal(principal interface{}, refType string) *DatabaseObject {
// 	// Get user object
// 	Key, _ := PrincipalMarshalling(principal)

// 	// Generate DatabaseObject without JSON-object in it.
// 	key := NewDatabaseObject(Key.Uuid, refType)

// 	return key
// }

// CreateRootKeyObject creates a new user with new API key when none exists when starting server
func CreateRootKeyObject(key *models.Key) strfmt.UUID {
	// Create key token
	token := GenerateUUID()

	// Do not set any parent

	// Set expiry to unlimited
	key.KeyExpiresUnix = -1

	// Get ips as v6
	var ips []string
	ifaces, _ := net.Interfaces()
	for _, i := range ifaces {
		addrs, _ := i.Addrs()
		for _, addr := range addrs {
			var ip net.IP
			switch v := addr.(type) {
			case *net.IPNet:
				ip = v.IP
			case *net.IPAddr:
				ip = v.IP
			}

			ipv6 := ip.To16()
			ips = append(ips, ipv6.String())
		}
	}

	key.IPOrigin = ips

	// Set chmod variables
	key.Read = true
	key.Write = true
	key.Delete = true
	key.Execute = true

	// Set Mail
	key.Email = "weaviate@weaviate.nl"

	// Print the key
	log.Println("INFO: No root key was found, a new root key is created. More info: https://github.com/weaviate/weaviate/blob/develop/README.md#authentication")
	log.Println("INFO: Auto set allowed IPs to: ", key.IPOrigin)
	log.Println("ROOTKEY=" + token)

	return token
}

// NowUnix returns the current Unix time
func NowUnix() int64 {
	return time.Now().UnixNano() / int64(time.Millisecond)
}

// GenerateUUID returns a new UUID
func GenerateUUID() strfmt.UUID {
	return strfmt.UUID(fmt.Sprintf("%v", gouuid.NewV4()))
}

// WhereStringToStruct is the 'compiler' for converting the filter/where query-string into a struct
func WhereStringToStruct(prop string, where string) (WhereQuery, error) {
	whereQuery := WhereQuery{}

	// Make a regex which can compile a string like 'firstName>=~John'
	re1, _ := regexp.Compile(`^([a-zA-Z0-9]*)([:<>!=]*)([~]*)([^~]*)$`)
	result := re1.FindStringSubmatch(where)

	// Set which property
	whereQuery.Property = prop
	if len(result[1]) > 1 && len(result[4]) != 0 {
		whereQuery.Property = fmt.Sprintf("%s.%s", prop, result[1])
	}

	// Set the operator
	switch result[2] {
	// When operator is "", put in 'Equal' as operator
	case ":", "", "=":
		whereQuery.Value.Operator = Equal
	case "!:", "!=":
		whereQuery.Value.Operator = NotEqual
	// TODO: https://github.com/weaviate/weaviate/issues/202
	// case ">":
	// 	whereQuery.Value.Operator = GreaterThan
	// case ">:", ">=":
	// 	whereQuery.Value.Operator = GreaterThanEqual
	// case "<":
	// 	whereQuery.Value.Operator = LessThan
	// case "<:", "<=":
	// 	whereQuery.Value.Operator = LessThanEqual
	default:
		return whereQuery, errors.New("invalid operator set in query")
	}

	// The wild cards
	whereQuery.Value.Contains = result[3] == "~"

	// Set the value itself
	if len(result[4]) == 0 {
		if len(result[1]) > 0 && len(result[2]) == 0 && len(result[3]) == 0 {
			// If only result[1] is set, just use that as search term.
			whereQuery.Value.Value = result[1]
		} else {
			// When value is "", throw error
			return whereQuery, errors.New("no value is set in the query")
		}
	} else {
		whereQuery.Value.Value = result[4]
	}

	return whereQuery, nil
}
