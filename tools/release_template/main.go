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

package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"

	"github.com/pkg/errors"
)

const configurationAPIOrigin = "https://configuration.semi.technology"

func main() {
	version := os.Getenv("VERSION")

	languages, err := getLanguages()
	fatal(err)

	languages, err = getModels(languages)
	fatal(err)

	printRelaeseNotes(languages, version)
}

type language struct {
	code  string
	label string
	model string
}

func printRelaeseNotes(languages []language, version string) {
	fmt.Printf("Docker image/tag: `semitechnologies/weaviate:%s`\n", version)
	fmt.Printf("See also: example docker-compose files in %s. ", makeLinks(languages, version))
	fmt.Printf("If you need to configure additional settings, you can also generate " +
		"a custom `docker-compose.yml` file [using the documentation]" +
		"(https://weaviate.io/developers/weaviate/installation/docker-compose).")
	fmt.Printf("\n## Breaking Changes\n*none*\n")
	fmt.Printf("\n## New Features\n*none*\n")
	fmt.Printf("\n## Fixes\n*none*\n")
}

func makeLinks(languages []language, version string) string {
	var out string

	for i, lang := range languages {
		if i != 0 {
			out += ", "
		}

		link := fmt.Sprintf("%s/docker-compose?weaviate_version=%s&language=%s&contextionary_model=%s",
			configurationAPIOrigin, version, lang.code, lang.model)
		out += fmt.Sprintf("[%s](%s)", lang.label, link)
	}

	return out
}

func getModels(languages []language) ([]language, error) {
	for i, lang := range languages {
		resp, err := http.Get(fmt.Sprintf(
			"%s/docker-compose/parameters/contextionary-models?language=%s",
			configurationAPIOrigin, lang.code))
		if err != nil {
			return nil, errors.Wrapf(err, "language %s", lang.label)
		}

		defer resp.Body.Close()
		bytes, err := io.ReadAll(resp.Body)
		if err != nil {
			return nil, errors.Wrapf(err, "language %s", lang.label)
		}

		model, err := extractModel(bytes)
		if err != nil {
			return nil, errors.Wrapf(err, "language %s", lang.label)
		}

		languages[i].model = model
	}

	return languages, nil
}

func extractModel(in []byte) (string, error) {
	var payload map[string]interface{}
	err := json.Unmarshal(in, &payload)
	if err != nil {
		return "", err
	}

	options := payload["items"].([]interface{})
	if len(options) < 1 {
		return "", fmt.Errorf("no model options for this language")
	}

	return options[0].(map[string]interface{})["value"].(string), nil
}

func getLanguages() ([]language, error) {
	codes := strings.Split(os.Getenv("LANGUAGES"), " ")

	out := make([]language, len(codes))
	for i, code := range codes {
		label, err := languageFullName(code)
		if err != nil {
			return nil, err
		}

		out[i] = language{
			code:  code,
			label: label,
		}
	}

	return out, nil
}

func fatal(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

func languageFullName(code string) (string, error) {
	switch code {
	case "en":
		return "English", nil
	case "de":
		return "German", nil
	case "it":
		return "Italian", nil
	case "cs":
		return "Czech", nil
	case "nl":
		return "Dutch", nil
	default:
		return "", fmt.Errorf("unknown language %s", code)
	}
}
