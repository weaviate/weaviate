package t2vbigram

import (
	"context"
	"fmt"
	"log"
	"regexp"
	"strings"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/moduletools"
)

func alphabetOrdinal(letter rune) int {
	return int(letter - 'a')
}

func ord(letter rune) int {
	return int(letter)
}

func stripNonAlphabets(input string) (string, error) {
	reg, err := regexp.Compile("[^a-zA-Z]+")
	if err != nil {
		return "", err
	}
	return reg.ReplaceAllString(input, ""), nil
}

func alphabet2Vector(input string) ([]float32, error) {
	// Strip everything out of the in that is not a letter
	// and convert to lower case
	in, err := stripNonAlphabets(input)
	if err != nil {
		return nil, err
	}
	in = strings.ToLower(in)
	vector := make([]float32, 26*26)
	for i := 0; i < len(in)-1; i++ {
		first := alphabetOrdinal(rune(in[i]))
		second := alphabetOrdinal(rune(in[i+1]))
		index := first*26 + second
		vector[index] = vector[index] + 1
	}
	var sum float32
	for _, v := range vector {
		sum += v
	}

	for i, v := range vector {
		vector[i] = v / sum
	}
	return vector, nil
}

// Maybe we should do this for bytes instead of letters?
func mod26Vector(input string) ([]float32, error) {
	input = strings.ToLower(input)
	vector := make([]float32, 26*26)
	for i := 0; i < len(input)-1; i++ {
		first := int(input[i]) % 26
		second := int(input[i+1]) % 26
		index := first*26 + second
		vector[index] = vector[index] + 1
	}

	vec := normaliseVector(vector)
	log.Printf("mod26Vector: %+v", vec)
	return vec, nil
}

func normaliseVector(vector []float32) []float32 {
	var sum float32
	for _, v := range vector {
		sum += v
	}

	for i, v := range vector {
		vector[i] = v / sum
	}
	return vector
}

func trigramVector(input string) ([]float32, error) {
	input = strings.ToLower(input)
	vector := make([]float32, 26*26*26)
	for i := 0; i < len(input)-2; i++ {
		first := ord(rune(input[i])) % 26
		second := ord(rune(input[i+1])) % 26
		third := ord(rune(input[i+2])) % 26
		index := first*26*26 + second*26 + third
		vector[index] = vector[index] + 1
	}

	return normaliseVector(vector), nil
}

func bytePairs2Vector(input string) ([]float32, error) {
	vector := make([]float32, 256*256)
	for i := 0; i < len(input)-1; i++ {
		bigram := input[i : i+2]

		index := int(bigram[0]) * int(bigram[1])
		vector[index] = vector[index] + 1
	}

	return normaliseVector(vector[1:]), nil // Max length is 16k-1
}


func (m *BigramModule) VectorizeObject(ctx context.Context, obj *models.Object, cfg moduletools.ClassConfig) ([]float32, models.AdditionalProperties, error) {
	var text string
	for _, prop := range obj.Properties.(map[string]interface{}) {
		text += fmt.Sprintf("%v", prop)
	}
	vector, error := m.VectorizeInput(ctx, text, cfg)
	return vector, nil, error
}



func (m *BigramModule) VectorizableProperties(cfg moduletools.ClassConfig) (bool, []string, error) {
	return true, nil, nil
}

func (m *BigramModule) VectorizeBatch(ctx context.Context, objs []*models.Object, skipObject []bool, cfg moduletools.ClassConfig) ([][]float32, []models.AdditionalProperties, map[int]error) {
	var (
		vectors [][]float32
		errors  = map[int]error{}
	)
	for i, obj := range objs {
		if skipObject[i] {
			continue
		}
		vector, _, err := m.VectorizeObject(ctx, obj, cfg)
		if err != nil {
			errors[i] = err
		}
		vectors = append(vectors, vector)
	}
	return vectors, nil, errors
}

