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

package dynamic

import (
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"strings"

	"github.com/alexedwards/argon2id"
)

const (
	DynUserIdentifier               = "v200" // has to be 4 chars so full key is divisible by 3 and Base64Encoding does not end in "="
	RandomBytesLength               = 32
	RandomBytesBase64Length         = 44
	UserIdentifierBytesLength       = 12
	UserIdentifierBytesBase64Length = 16
)

// second recommendation from the RFC: https://www.rfc-editor.org/rfc/rfc9106.html#name-parameter-choice
// Changing ANY of these parameters will change the output and make previously generated keys invalid.
var argonParameters = &argon2id.Params{
	Memory:      64 * 1024,
	Parallelism: 2,
	Iterations:  3,
	SaltLength:  16,
	KeyLength:   32,
}

// CreateApiKeyAndHash creates an api key that has three parts:
// 1) an argon hash of a random key with length 32 bytes (Base64 encoded)
// 2) a random user identifier with length 10 bytes (Base64 encoded)
//   - this identifier can be used to fetch the hash later
//
// 3) a version string to identify the type of api key
//
// The different parts have "_" as separator (which is not part of Base64) and the combined string is encoded again as
// Base64 to be returned to the user. The apiKey length is divisible by 3 so the Base64Encoding does not end in "=".
//
// To verify that a user provides the correct key the following steps have to be taken:
//   - decode the key into the 3 parts mentioned above using DecodeApiKey
//   - fetch the saved hash based on the returned user identifier
//   - compare the returned randomKey with the fetched hash using argon2id.ComparePasswordAndHash
func CreateApiKeyAndHash() (string, string, string, error) {
	randomBytesKey, err := generateRandomBytes(RandomBytesLength)
	if err != nil {
		return "", "", "", err
	}
	randomKey := base64.StdEncoding.EncodeToString(randomBytesKey)

	randomBytesIdentifier, err := generateRandomBytes(UserIdentifierBytesLength)
	if err != nil {
		return "", "", "", err
	}
	userIdentifier := base64.StdEncoding.EncodeToString(randomBytesIdentifier)

	fullApiKey := generateApiKey(randomKey, userIdentifier)

	hash, err := argon2id.CreateHash(randomKey, argonParameters)

	return fullApiKey, hash, userIdentifier, err
}

func generateApiKey(randomKey, userIdentifier string) string {
	fullString := randomKey + "_" + userIdentifier + "_" + DynUserIdentifier
	return base64.StdEncoding.EncodeToString([]byte(fullString))
}

func generateRandomBytes(length int) ([]byte, error) {
	b := make([]byte, length)
	_, err := rand.Read(b)
	// Note that err == nil only if we read len(b) bytes.
	if err != nil {
		return nil, err
	}

	return b, nil
}

func DecodeApiKey(fullApiKey string) (string, string, error) {
	decodeString, err := base64.StdEncoding.DecodeString(fullApiKey)
	if err != nil {
		return "", "", err
	}

	parts := strings.Split(string(decodeString), "_")
	if len(parts) != 3 {
		return "", "", fmt.Errorf("invalid token")
	}

	randomKey := parts[0]
	userIdentifier := parts[1]
	version := parts[2]
	if version != DynUserIdentifier {
		return "", "", fmt.Errorf("invalid token")
	}

	if len(userIdentifier) != UserIdentifierBytesBase64Length {
		return "", "", fmt.Errorf("invalid token")
	}

	if len(randomKey) != RandomBytesBase64Length {
		return "", "", fmt.Errorf("invalid token")
	}

	return randomKey, userIdentifier, nil
}
