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
	"fmt"
	"strconv"
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

func GetValueFromContext(ctx context.Context, key string) string {
	if value := ctx.Value(key); value != nil {
		if keyHeader, ok := value.([]string); ok && len(keyHeader) > 0 && len(keyHeader[0]) > 0 {
			return keyHeader[0]
		}
	}
	// try getting header from GRPC if not successful
	if value := GetValueFromGRPC(ctx, key); len(value) > 0 && len(value[0]) > 0 {
		return value[0]
	}

	return ""
}

func GetRateLimitFromContext(ctx context.Context, moduleName string, defaultRPM, defaultTPM int) (int, int) {
	returnRPM := defaultRPM
	returnTPM := defaultTPM
	if rpmS := GetValueFromContext(ctx, fmt.Sprintf("X-%s-Ratelimit-RequestPM-Embedding", moduleName)); rpmS != "" {
		s, err := strconv.Atoi(rpmS)
		if err == nil {
			returnRPM = s
		}
	}
	if tpmS := GetValueFromContext(ctx, fmt.Sprintf("X-%s-Ratelimit-TokenPM-Embedding", moduleName)); tpmS != "" {
		s, err := strconv.Atoi(tpmS)
		if err == nil {
			returnTPM = s
		}
	}

	return returnRPM, returnTPM
}
