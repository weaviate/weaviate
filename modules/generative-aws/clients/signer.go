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

package clients

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"
)

const (
	contentType = "application/json"
	algorithm   = "AWS4-HMAC-SHA256"
)

func getAuthHeader(awsAccessKey string, awsSecretKey string, host string, service string, region string, path string, body []byte, headers map[string]string) (string, map[string]string, string) {
	t := time.Now().UTC()
	amzDate := t.Format("20060102T150405Z")
	shortDate := t.Format("20060102")

	hashedPayload := sha256Hash(body)

	canonicalHeaders, signedHeaders := getCanonicalHeaders(headers)

	canonicalRequest := strings.Join([]string{
		http.MethodPost,
		path,
		"",
		canonicalHeaders,
		signedHeaders,
		hashedPayload,
	}, "\n")

	hashedCanonicalRequest := sha256Hash([]byte(canonicalRequest))

	credentialScope := strings.Join([]string{shortDate, region, service, "aws4_request"}, "/")

	stringToSign := strings.Join([]string{
		algorithm,
		amzDate,
		credentialScope,
		hashedCanonicalRequest,
	}, "\n")

	signingKey := getSigningKey(awsSecretKey, shortDate, region, service)

	signature := hmacSHA256(signingKey, stringToSign)

	authorizationHeader := fmt.Sprintf("%s Credential=%s/%s, SignedHeaders=%s, Signature=%s", algorithm, awsAccessKey, credentialScope, signedHeaders, signature)
	return amzDate, headers, authorizationHeader
}

func getCanonicalHeaders(headers map[string]string) (string, string) {
	var canonicalHeaders []string
	var signedHeaders []string

	keys := make([]string, 0, len(headers))
	for k := range headers {
		keys = append(keys, k)
	}

	sort.Strings(keys)

	for _, k := range keys {
		canonicalHeaders = append(canonicalHeaders, fmt.Sprintf("%s:%s\n", strings.ToLower(k), headers[k]))
		signedHeaders = append(signedHeaders, strings.ToLower(k))
	}

	return strings.Join(canonicalHeaders, ""), strings.Join(signedHeaders, ";")
}

func hmacSHA256(key []byte, message string) string {
	mac := hmac.New(sha256.New, key)
	mac.Write([]byte(message))
	return hex.EncodeToString(mac.Sum(nil))
}

func getSigningKey(secretKey, date, region, service string) []byte {
	key := "AWS4" + secretKey
	kDate := hmacSHA256Bytes([]byte(key), date)
	kRegion := hmacSHA256Bytes(kDate, region)
	kService := hmacSHA256Bytes(kRegion, service)
	kSigning := hmacSHA256Bytes(kService, "aws4_request")
	return kSigning
}

func hmacSHA256Bytes(key []byte, message string) []byte {
	mac := hmac.New(sha256.New, key)
	mac.Write([]byte(message))
	return mac.Sum(nil)
}

func sha256Hash(body []byte) string {
	hash := sha256.Sum256(body)
	return hex.EncodeToString(hash[:])
}
