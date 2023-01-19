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
