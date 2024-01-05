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

package modulecomponents

import (
	"context"
	"strings"

	"google.golang.org/grpc/metadata"
)

func GetValueFromGRPC(ctx context.Context, key string) []string {
	md, ok := metadata.FromIncomingContext(ctx)
	if ok {
		// the grpc library will lowercase all md keys, so we need to make sure to check a lowercase key
		apiKey, ok := md[strings.ToLower(key)]
		if ok {
			return apiKey
		}
	}
	return nil
}
