//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2019 SeMI Holding B.V. (registered @ Dutch Chamber of Commerce no 75221632). All rights reserved.
//  LICENSE WEAVIATE OPEN SOURCE: https://www.semi.technology/playbook/playbook/contract-weaviate-OSS.html
//  LICENSE WEAVIATE ENTERPRISE: https://www.semi.technology/playbook/contract-weaviate-enterprise.html
//  CONCEPT: Bob van Luijt (@bobvanluijt)
//  CONTACT: hello@semi.technology
//

package classification

import (
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/go-openapi/strfmt"
	libfilters "github.com/semi-technologies/weaviate/entities/filters"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/entities/schema/crossref"
	"github.com/semi-technologies/weaviate/entities/schema/kind"
	"github.com/semi-technologies/weaviate/entities/search"
	"github.com/semi-technologies/weaviate/usecases/traverser"
)

type contextualItemClassifier struct {
	item        search.Result
	itemIndex   int
	kind        kind.Kind
	params      models.Classification
	classifier  *Classifier
	schema      schema.Schema
	filters     filters
	context     contextualPreparationContext
	vectorizer  vectorizer
	words       []string
	rankedWords map[string][]scoredWord // map[targetProp]words as scoring/ranking is per target
}

// makeClassifyItemContextual is a higher-order function to produce the actual
// classify function, but additionally allows us to inject data which is valid
// for the entire run, such as tf-idf data and target vectors
func (c *Classifier) makeClassifyItemContextual(preparedContext contextualPreparationContext) func(search.Result,
	int, kind.Kind, models.Classification, filters) error {
	return func(item search.Result, itemIndex int, kind kind.Kind, params models.Classification, filters filters) error {
		schema := c.schemaGetter.GetSchemaSkipAuth()
		run := &contextualItemClassifier{
			item:        item,
			itemIndex:   itemIndex,
			kind:        kind,
			params:      params,
			classifier:  c,
			schema:      schema,
			filters:     filters,
			context:     preparedContext,
			vectorizer:  c.vectorizer,
			rankedWords: map[string][]scoredWord{},
		}

		err := run.do()
		if err != nil {
			return fmt.Errorf("contextual: %v", err)
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
	err := c.classifier.store(c.item)
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
	if !ok {
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
	vector, err := c.vectorizer.VectorForCorpi(ctx, []string{corpus}, boosts)
	if err != nil {
		return "", fmt.Errorf("vectorize corpus: %v", err)
	}

	target, distance, err := c.findClosestTarget(vector, propName)
	if err != nil {
		return "", fmt.Errorf("find closest target: %v", err)
	}

	// targetClass, targetKind, err := c.classAndKindOfTarget(propName)
	// if err != nil {
	// 	return "", fmt.Errorf("inspect target: %v", err)
	// }

	// res, err := c.findTarget(targetClass, targetKind)
	// if err != nil {
	// 	return "", fmt.Errorf("find target: %v", err)
	// }

	// distance, err := c.distance(res.Vector)
	// if err != nil {
	// 	return "", fmt.Errorf("calculate distance: %v", err)
	// }

	targetBeacon := crossref.New("localhost", target.ID, target.Kind).String()
	c.item.Schema.(map[string]interface{})[propName] = models.MultipleRef{
		&models.SingleRef{
			Beacon: strfmt.URI(targetBeacon),
			Meta: &models.ReferenceMeta{
				Classification: &models.ReferenceMetaClassification{
					WinningDistance: float64(distance),
				},
			},
		},
	}

	return propName, nil
}

func (c *contextualItemClassifier) findClosestTarget(query []float32, targetProp string) (*search.Result, float32, error) {
	var minimum = float32(100000)
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
		// TODO: ig cutoff percentile
		// TODO: tfidf cutoff percentile
		if c.isInIgPercentile(10, word, targetProp) && c.isInTfPercentile(tfscores, 80, word) {
			corpus = append(corpus, word)
		}
	}

	// use minimum words if len is currently less
	// TODO: use actual parameters
	limit := 3
	if len(corpus) < limit {
		corpus = c.getTopNWords(targetProp, limit)
	}

	corpusStr := strings.ToLower(strings.Join(corpus, " "))
	// TODO: ig cutfoff percentile
	// TODO: max boost
	boosts := c.boostByInformationGain(targetProp, 10, 3)
	// TODO: add overrides
	return corpusStr, boosts, nil
}

func (c *contextualItemClassifier) boostByInformationGain(targetProp string, percentile int,
	maxBoost float32) map[string]string {
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

	out := make([]string, limit+1)
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
	targetProp string) ([]*scoredWord, error) {
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
	targetProp string) (*scoredWord, error) {
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

func (c *contextualItemClassifier) classAndKindOfTarget(propName string) (schema.ClassName, kind.Kind, error) {
	prop, err := c.schema.GetProperty(c.kind, schema.ClassName(c.params.Class), schema.PropertyName(propName))
	if err != nil {
		return "", "", fmt.Errorf("get target prop '%s': %v", propName, err)
	}

	dataType, err := c.schema.FindPropertyDataType(prop.DataType)
	if err != nil {
		return "", "", fmt.Errorf("extract dataType of prop '%s': %v", propName, err)
	}

	// we have passed validation, so it is safe to assume that this is a ref prop
	targetClasses := dataType.Classes()

	// len=1 is guaranteed from validation
	targetClass := targetClasses[0]
	targetKind, _ := c.schema.GetKindOfClass(targetClass)

	return targetClass, targetKind, nil
}

func (c *contextualItemClassifier) findTarget(targetClass schema.ClassName, targetKind kind.Kind) (*search.Result, error) {
	ctx, cancel := contextWithTimeout(2 * time.Second)
	defer cancel()

	res, err := c.classifier.vectorRepo.VectorClassSearch(ctx, traverser.GetParams{
		SearchVector: c.item.Vector,
		ClassName:    targetClass.String(),
		Kind:         targetKind,
		Pagination: &libfilters.Pagination{
			Limit: 1,
		},
		Filters: c.filters.target,
		Properties: traverser.SelectProperties{
			traverser.SelectProperty{
				Name: "uuid",
			},
		},
	})
	if err != nil {
		return nil, fmt.Errorf("search closest target: %v", err)
	}

	if res == nil || len(res) == 0 {
		return nil, fmt.Errorf("no potential targets found of class '%s' (%s)", targetClass, targetKind)
	}

	return &res[0], nil
}

func (c *contextualItemClassifier) distance(target []float32) (float64, error) {
	dist, err := c.classifier.distancer(c.item.Vector, target)
	return float64(dist), err
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
