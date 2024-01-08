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

package lib

import (
	"net/url"

	client "github.com/weaviate/weaviate-go-client/v4/weaviate"
)

func ClientFromOrigin(origin string) (*client.Client, error) {
	parsed, err := url.Parse(origin)
	if err != nil {
		return nil, err
	}

	config := client.Config{
		Scheme: parsed.Scheme,
		Host:   parsed.Host,
	}

	client := client.New(config)

	return client, nil
}
