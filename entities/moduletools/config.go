//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package moduletools

// ClassConfig is a helper type which is passed to the module to read it's
// per-class config. This is - among other places - used when vectorizing and
// when validation schema config
type ClassConfig interface {
	Class() map[string]interface{}
	Property(propName string) map[string]interface{}
}
