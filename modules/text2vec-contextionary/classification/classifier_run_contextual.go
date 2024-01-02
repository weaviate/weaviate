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

package classification

import (
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/schema/crossref"
	"github.com/weaviate/weaviate/entities/search"
)

// TODO: all of this must be served by the module in the future
type contextualItemClassifier struct {
	item        search.Result
	itemIndex   int
	params      models.Classification
	settings    *ParamsContextual
	classifier  *Classifier
	writer      modulecapabilities.Writer
	schema      schema.Schema
	filters     modulecapabilities.Filters
	context     contextualPreparationContext
	vectorizer  vectorizer
	words       []string
	rankedWords map[string][]scoredWord // map[targetProp]words as scoring/ranking is per target
}

func (c *Classifier) extendItemWithObjectMeta(item *search.Result,
	params models.Classification, classified []string,
) {
	// don't overwrite existing non-classification meta info
	if item.AdditionalProperties == nil {
		item.AdditionalProperties = models.AdditionalProperties{}
	}

	item.AdditionalProperties["classification"] = additional.Classification{
		ID:               params.ID,
		Scope:            params.ClassifyProperties,
		ClassifiedFields: classified,
		Completed:        strfmt.DateTime(time.Now()),
	}
}

// makeClassifyItemContextual is a higher-order function to produce the actual
// classify function, but additionally allows us to inject data which is valid
// for the entire run, such as tf-idf data and target vectors
func (c *Classifier) makeClassifyItemContextual(schema schema.Schema, preparedContext contextualPreparationContext) func(search.Result,
	int, models.Classification, modulecapabilities.Filters, modulecapabilities.Writer) error {
	return func(item search.Result, itemIndex int, params models.Classification,
		filters modulecapabilities.Filters, writer modulecapabilities.Writer,
	) error {
		vectorizer := c.vectorizer
		run := &contextualItemClassifier{
			item:        item,
			itemIndex:   itemIndex,
			params:      params,
			settings:    params.Settings.(*ParamsContextual), // safe assertion after parsing
			classifier:  c,
			writer:      writer,
			schema:      schema,
			filters:     filters,
			context:     preparedContext,
			vectorizer:  vectorizer,
			rankedWords: map[string][]scoredWord{},
		}

		err := run.do()
		if err != nil {
			return fmt.Errorf("text2vec-contextionary-contextual: %v", err)
		}

		return nil
	}
}

func (c *contextualItemClassifier) do() error {
	var classified []string
	for _, propName := range c.params.ClassifyProperties {
		current, err := c.property(propName)
		if err != nil {
			return fmt.Errorf("prop '%s': %v", propName, err)
		}

		// append list of actually classified (can differ from scope!) properties,
		// so we can build the object meta information
		classified = append(classified, current)
	}

	c.classifier.extendItemWithObjectMeta(&c.item, c.params, classified)
	err := c.writer.Store(c.item)
	if err != nil {
		return fmt.Errorf("store %s/%s: %v", c.item.ClassName, c.item.ID, err)
	}

	return nil
}

func (c *contextualItemClassifier) property(propName string) (string, error) {
	targets, ok := c.context.targets[propName]
	if !ok || len(targets) == 0 {
		return "", fmt.Errorf("have no potential targets for property '%s'", propName)
	}

	schemaMap, ok := c.item.Schema.(map[string]interface{})
	if !ok {
		return "", fmt.Errorf("no or incorrect schema map present on source c.object '%s': %T", c.item.ID, c.item.Schema)
	}

	// Limitation for now, basedOnProperty is always 0
	basedOnName := c.params.BasedOnProperties[0]
	basedOn, ok := schemaMap[basedOnName]
	if !ok {
		return "", fmt.Errorf("property '%s' not found on source c.object '%s': %T", propName, c.item.ID, c.item.Schema)
	}

	basedOnString, ok := basedOn.(string)
	if !ok {
		return "", fmt.Errorf("property '%s' present on %s, but of unexpected type: want string, got %T",
			basedOnName, c.item.ID, basedOn)
	}

	words := newSplitter().Split(basedOnString)
	c.words = words

	ctx, cancel := contextWithTimeout(10 * time.Second)
	defer cancel()

	vectors, err := c.vectorizer.MultiVectorForWord(ctx, words)
	if err != nil {
		return "", fmt.Errorf("vectorize individual words: %v", err)
	}

	scoredWords, err := c.scoreWords(words, vectors, propName)
	if err != nil {
		return "", fmt.Errorf("score words: %v", err)
	}

	c.rankedWords[propName] = c.rankAndDedup(scoredWords)

	corpus, boosts, err := c.buildBoostedCorpus(propName)
	if err != nil {
		return "", fmt.Errorf("build corpus: %v", err)
	}

	ctx, cancel = contextWithTimeout(10 * time.Second)
	defer cancel()
	vector, err := c.vectorizer.VectorOnlyForCorpi(ctx, []string{corpus}, boosts)
	if err != nil {
		return "", fmt.Errorf("vectorize corpus: %v", err)
	}

	target, distance, err := c.findClosestTarget(vector, propName)
	if err != nil {
		return "", fmt.Errorf("find closest target: %v", err)
	}

	targetBeacon := crossref.New("localhost", target.ClassName, target.ID).String()
	c.item.Schema.(map[string]interface{})[propName] = models.MultipleRef{
		&models.SingleRef{
			Beacon: strfmt.URI(targetBeacon),
			Classification: &models.ReferenceMetaClassification{
				WinningDistance: float64(distance),
			},
		},
	}

	return propName, nil
}

func (c *contextualItemClassifier) findClosestTarget(query []float32, targetProp string) (*search.Result, float32, error) {
	minimum := float32(100000)
	var prediction search.Result

	for _, item := range c.context.targets[targetProp] {
		dist, err := cosineDist(query, item.Vector)
		if err != nil {
			return nil, -1, fmt.Errorf("calculate distance: %v", err)
		}

		if dist < minimum {
			minimum = dist
			prediction = item
		}
	}

	return &prediction, minimum, nil
}

func (c *contextualItemClassifier) buildBoostedCorpus(targetProp string) (string, map[string]string, error) {
	var corpus []string

	for _, word := range c.words {
		word = strings.ToLower(word)

		tfscores := c.context.tfidf[c.params.BasedOnProperties[0]].GetAllTerms(c.itemIndex)
		// dereferencing these optional parameters is safe, as defaults are
		// explicitly set in classifier.Schedule()
		if c.isInIgPercentile(int(*c.settings.InformationGainCutoffPercentile), word, targetProp) &&
			c.isInTfPercentile(tfscores, int(*c.settings.TfidfCutoffPercentile), word) {
			corpus = append(corpus, word)
		}
	}

	// use minimum words if len is currently less
	limit := int(*c.settings.MinimumUsableWords)
	if len(corpus) < limit {
		corpus = c.getTopNWords(targetProp, limit)
	}

	corpusStr := strings.ToLower(strings.Join(corpus, " "))
	boosts := c.boostByInformationGain(targetProp, int(*c.settings.InformationGainCutoffPercentile),
		float32(*c.settings.InformationGainMaximumBoost))
	return corpusStr, boosts, nil
}

func (c *contextualItemClassifier) boostByInformationGain(targetProp string, percentile int,
	maxBoost float32,
) map[string]string {
	cutoff := int(float32(percentile) / float32(100) * float32(len(c.rankedWords[targetProp])))
	out := make(map[string]string, cutoff)

	for i, word := range c.rankedWords[targetProp][:cutoff] {
		boost := 1 - float32(math.Log(float64(i)/float64(cutoff)))*float32(1)
		if math.IsInf(float64(boost), 1) || boost > maxBoost {
			boost = maxBoost
		}

		out[word.word] = fmt.Sprintf("%f * w", boost)
	}

	return out
}

type scoredWord struct {
	word            string
	distance        float32
	informationGain float32
}

func (c *contextualItemClassifier) getTopNWords(targetProp string, limit int) []string {
	words := c.rankedWords[targetProp]

	if len(words) < limit {
		limit = len(words)
	}

	out := make([]string, limit)
	for i := 0; i < limit; i++ {
		out[i] = words[i].word
	}

	return out
}

func (c *contextualItemClassifier) rankAndDedup(in []*scoredWord) []scoredWord {
	return c.dedup(c.rank(in))
}

func (c *contextualItemClassifier) dedup(in []scoredWord) []scoredWord {
	// simple dedup since it's already ordered, we only need to check the previous element
	indexOut := 0
	out := make([]scoredWord, len(in))
	for i, elem := range in {
		if i == 0 {
			out[indexOut] = elem
			indexOut++
			continue
		}

		if elem.word == out[indexOut-1].word {
			continue
		}

		out[indexOut] = elem
		indexOut++
	}

	return out[:indexOut]
}

func (c *contextualItemClassifier) rank(in []*scoredWord) []scoredWord {
	i := 0
	filtered := make([]scoredWord, len(in))
	for _, w := range in {
		if w == nil {
			continue
		}

		filtered[i] = *w
		i++
	}
	out := filtered[:i]
	sort.Slice(out, func(a, b int) bool { return out[a].informationGain > out[b].informationGain })
	return out
}

func (c *contextualItemClassifier) scoreWords(words []string, vectors [][]float32,
	targetProp string,
) ([]*scoredWord, error) {
	if len(words) != len(vectors) {
		return nil, fmt.Errorf("fatal: word list (l=%d) and vector list (l=%d) have different lengths",
			len(words), len(vectors))
	}

	out := make([]*scoredWord, len(words))
	for i := range words {
		word := strings.ToLower(words[i])
		sw, err := c.scoreWord(word, vectors[i], targetProp)
		if err != nil {
			return nil, fmt.Errorf("score word '%s': %v", word, err)
		}

		// accept nil-entries for now, they will be removed in ranking/deduping
		out[i] = sw
	}

	return out, nil
}

func (c *contextualItemClassifier) scoreWord(word string, vector []float32,
	targetProp string,
) (*scoredWord, error) {
	var all []float32
	minimum := float32(1000000.00)

	if vector == nil {
		return nil, nil
	}

	targets, ok := c.context.targets[targetProp]
	if !ok {
		return nil, fmt.Errorf("fatal: targets for prop '%s' not found", targetProp)
	}

	for _, target := range targets {
		dist, err := cosineDist(vector, target.Vector)
		if err != nil {
			return nil, fmt.Errorf("calculate cosine distance: %v", err)
		}

		all = append(all, dist)

		if dist < minimum {
			minimum = dist
		}
	}

	return &scoredWord{word: word, distance: minimum, informationGain: avg(all) - minimum}, nil
}

func avg(in []float32) float32 {
	var sum float32
	for _, curr := range in {
		sum += curr
	}

	return sum / float32(len(in))
}

func (c *contextualItemClassifier) isInIgPercentile(percentage int, needle string, target string) bool {
	cutoff := int(float32(percentage) / float32(100) * float32(len(c.rankedWords[target])))

	// no need to check if key exists, guaranteed from run
	selection := c.rankedWords[target][:cutoff]

	for _, hay := range selection {
		if needle == hay.word {
			return true
		}
	}

	return false
}

func (c *contextualItemClassifier) isInTfPercentile(tf []TermWithTfIdf, percentage int, needle string) bool {
	cutoff := int(float32(percentage) / float32(100) * float32(len(tf)))
	selection := tf[:cutoff]

	for _, hay := range selection {
		if needle == hay.Term {
			return true
		}
	}

	return false
}

func cosineSim(a, b []float32) (float32, error) {
	if len(a) != len(b) {
		return 0, fmt.Errorf("vectors have different dimensions")
	}

	var (
		sumProduct float64
		sumASquare float64
		sumBSquare float64
	)

	for i := range a {
		sumProduct += float64(a[i] * b[i])
		sumASquare += float64(a[i] * a[i])
		sumBSquare += float64(b[i] * b[i])
	}

	return float32(sumProduct / (math.Sqrt(sumASquare) * math.Sqrt(sumBSquare))), nil
}

func cosineDist(a, b []float32) (float32, error) {
	sim, err := cosineSim(a, b)
	if err != nil {
		return 0, err
	}

	return 1 - sim, nil
}
