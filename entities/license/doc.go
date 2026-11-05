//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

// Package license defines Weaviate's license protocol: the customer-facing
// key format, canonical JSON signing of verify requests, and server-signed
// verify responses. Weaviate instances use it to parse license keys and to
// communicate with the Weaviate license service. It depends only on the
// standard library.
package license
