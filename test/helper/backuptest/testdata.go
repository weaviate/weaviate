//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package backuptest

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
)

// TestDataConfig contains configuration for generating test data.
type TestDataConfig struct {
	// ClassName is the name of the class to create.
	ClassName string

	// MultiTenant enables multi-tenancy for the class.
	MultiTenant bool

	// NumTenants is the number of tenants to create (only used if MultiTenant is true).
	NumTenants int

	// ObjectsPerTenant is the number of objects to create per tenant.
	// If MultiTenant is false, this is the total number of objects.
	ObjectsPerTenant int

	// Seed for random number generator. Use 0 for time-based seed.
	Seed int64

	// UseVectorizer specifies which vectorizer module to use.
	// Empty string means no vectorizer (manual vectors).
	UseVectorizer string
}

// DefaultTestDataConfig returns a default configuration for test data generation.
func DefaultTestDataConfig() *TestDataConfig {
	return &TestDataConfig{
		ClassName:        "BackupTestClass",
		MultiTenant:      false,
		NumTenants:       5,
		ObjectsPerTenant: 10,
		Seed:             0,
		UseVectorizer:    "", // No vectorizer by default
	}
}

// TestDataGenerator generates test classes, objects, and tenants.
type TestDataGenerator struct {
	config *TestDataConfig
	rng    *rand.Rand
}

// NewTestDataGenerator creates a new TestDataGenerator with the given config.
func NewTestDataGenerator(config *TestDataConfig) *TestDataGenerator {
	if config == nil {
		config = DefaultTestDataConfig()
	}

	seed := config.Seed
	if seed == 0 {
		seed = time.Now().UnixNano()
	}

	return &TestDataGenerator{
		config: config,
		rng:    rand.New(rand.NewSource(seed)),
	}
}

// GenerateClass creates a class schema for backup testing.
func (g *TestDataGenerator) GenerateClass() *models.Class {
	class := &models.Class{
		Class: g.config.ClassName,
		Properties: []*models.Property{
			{
				Name:         "title",
				DataType:     schema.DataTypeText.PropString(),
				Tokenization: models.PropertyTokenizationWhitespace,
			},
			{
				Name:         "content",
				DataType:     schema.DataTypeText.PropString(),
				Tokenization: models.PropertyTokenizationWord,
			},
			{
				Name:     "count",
				DataType: schema.DataTypeInt.PropString(),
			},
			{
				Name:     "score",
				DataType: schema.DataTypeNumber.PropString(),
			},
			{
				Name:     "active",
				DataType: schema.DataTypeBoolean.PropString(),
			},
			{
				Name:     "tags",
				DataType: schema.DataTypeTextArray.PropString(),
			},
		},
	}

	if g.config.MultiTenant {
		class.MultiTenancyConfig = &models.MultiTenancyConfig{
			Enabled: true,
		}
	}

	if g.config.UseVectorizer != "" {
		class.Vectorizer = g.config.UseVectorizer
		class.ModuleConfig = map[string]interface{}{
			g.config.UseVectorizer: map[string]interface{}{
				// Don't vectorize class name - our generated class names contain
				// underscores and timestamps that aren't in contextionary's vocabulary
				"vectorizeClassName": false,
			},
		}
	}

	return class
}

// GenerateTenants creates a list of tenant names.
func (g *TestDataGenerator) GenerateTenants() []string {
	if !g.config.MultiTenant {
		return nil
	}

	tenants := make([]string, g.config.NumTenants)
	for i := 0; i < g.config.NumTenants; i++ {
		tenants[i] = fmt.Sprintf("tenant-%d", i)
	}
	return tenants
}

// GenerateTenantModels creates tenant models for the Weaviate API.
func (g *TestDataGenerator) GenerateTenantModels() []*models.Tenant {
	tenantNames := g.GenerateTenants()
	if tenantNames == nil {
		return nil
	}

	tenants := make([]*models.Tenant, len(tenantNames))
	for i, name := range tenantNames {
		tenants[i] = &models.Tenant{
			Name:           name,
			ActivityStatus: models.TenantActivityStatusHOT,
		}
	}
	return tenants
}

// GenerateObject creates a single test object with random data.
func (g *TestDataGenerator) GenerateObject(tenantName string) *models.Object {
	obj := &models.Object{
		Class: g.config.ClassName,
		ID:    strfmt.UUID(uuid.New().String()),
		Properties: map[string]interface{}{
			"title":   g.randomTitle(),
			"content": g.randomContent(),
			"count":   g.rng.Intn(1000),
			"score":   g.rng.Float64() * 100,
			"active":  g.rng.Intn(2) == 1,
			"tags":    g.randomTags(),
		},
	}

	if tenantName != "" {
		obj.Tenant = tenantName
	}

	return obj
}

// GenerateObjects creates a batch of test objects.
func (g *TestDataGenerator) GenerateObjects(tenantName string, count int) []*models.Object {
	objects := make([]*models.Object, count)
	for i := 0; i < count; i++ {
		objects[i] = g.GenerateObject(tenantName)
	}
	return objects
}

// GenerateAllObjects creates objects for all tenants (or a single batch if not multi-tenant).
func (g *TestDataGenerator) GenerateAllObjects() []*models.Object {
	if g.config.MultiTenant {
		var allObjects []*models.Object
		tenants := g.GenerateTenants()
		for _, tenant := range tenants {
			objects := g.GenerateObjects(tenant, g.config.ObjectsPerTenant)
			allObjects = append(allObjects, objects...)
		}
		return allObjects
	}

	return g.GenerateObjects("", g.config.ObjectsPerTenant)
}

// ObjectsByTenant groups objects by tenant name.
// For non-multi-tenant classes, all objects are under the empty string key.
func (g *TestDataGenerator) ObjectsByTenant(objects []*models.Object) map[string][]*models.Object {
	result := make(map[string][]*models.Object)
	for _, obj := range objects {
		tenant := obj.Tenant
		result[tenant] = append(result[tenant], obj)
	}
	return result
}

// randomTitle generates a random title string.
func (g *TestDataGenerator) randomTitle() string {
	adjectives := []string{"Quick", "Lazy", "Clever", "Happy", "Sad", "Bright", "Dark", "Swift", "Slow", "Ancient"}
	nouns := []string{"Fox", "Dog", "Cat", "Bird", "Tree", "River", "Mountain", "Cloud", "Star", "Book"}

	adj := adjectives[g.rng.Intn(len(adjectives))]
	noun := nouns[g.rng.Intn(len(nouns))]

	return fmt.Sprintf("The %s %s %d", adj, noun, g.rng.Intn(100))
}

// randomContent generates random content string.
func (g *TestDataGenerator) randomContent() string {
	sentences := []string{
		"Lorem ipsum dolor sit amet consectetur adipiscing elit.",
		"The quick brown fox jumps over the lazy dog.",
		"All that glitters is not gold.",
		"A journey of a thousand miles begins with a single step.",
		"Knowledge is power.",
		"Time flies like an arrow; fruit flies like a banana.",
		"To be or not to be, that is the question.",
		"The only thing we have to fear is fear itself.",
		"In the beginning, there was nothing.",
		"Life is what happens when you're busy making other plans.",
	}

	// Generate 1-3 sentences
	count := 1 + g.rng.Intn(3)
	result := ""
	for i := 0; i < count; i++ {
		if i > 0 {
			result += " "
		}
		result += sentences[g.rng.Intn(len(sentences))]
	}
	return result
}

// randomTags generates a random list of tags.
func (g *TestDataGenerator) randomTags() []string {
	allTags := []string{
		"important", "urgent", "review", "draft", "final",
		"backend", "frontend", "database", "api", "testing",
		"bug", "feature", "enhancement", "documentation", "refactor",
	}

	// Generate 1-4 tags
	count := 1 + g.rng.Intn(4)
	tags := make([]string, count)
	for i := 0; i < count; i++ {
		tags[i] = allTags[g.rng.Intn(len(allTags))]
	}
	return tags
}

// TotalObjectCount returns the total number of objects that will be generated.
func (g *TestDataGenerator) TotalObjectCount() int {
	if g.config.MultiTenant {
		return g.config.NumTenants * g.config.ObjectsPerTenant
	}
	return g.config.ObjectsPerTenant
}

// Config returns the current configuration.
func (g *TestDataGenerator) Config() *TestDataConfig {
	return g.config
}

// WithClassName returns a modified generator with a new class name.
func (g *TestDataGenerator) WithClassName(name string) *TestDataGenerator {
	newConfig := *g.config
	newConfig.ClassName = name
	return NewTestDataGenerator(&newConfig)
}

// WithMultiTenant returns a modified generator with multi-tenancy enabled/disabled.
func (g *TestDataGenerator) WithMultiTenant(enabled bool) *TestDataGenerator {
	newConfig := *g.config
	newConfig.MultiTenant = enabled
	return NewTestDataGenerator(&newConfig)
}

// WithNumTenants returns a modified generator with a different tenant count.
func (g *TestDataGenerator) WithNumTenants(count int) *TestDataGenerator {
	newConfig := *g.config
	newConfig.NumTenants = count
	return NewTestDataGenerator(&newConfig)
}

// WithObjectsPerTenant returns a modified generator with a different object count.
func (g *TestDataGenerator) WithObjectsPerTenant(count int) *TestDataGenerator {
	newConfig := *g.config
	newConfig.ObjectsPerTenant = count
	return NewTestDataGenerator(&newConfig)
}

// WithVectorizer returns a modified generator with a vectorizer module.
func (g *TestDataGenerator) WithVectorizer(vectorizer string) *TestDataGenerator {
	newConfig := *g.config
	newConfig.UseVectorizer = vectorizer
	return NewTestDataGenerator(&newConfig)
}

// WithSeed returns a modified generator with a specific seed for reproducibility.
func (g *TestDataGenerator) WithSeed(seed int64) *TestDataGenerator {
	newConfig := *g.config
	newConfig.Seed = seed
	return NewTestDataGenerator(&newConfig)
}

// FixedTestData contains pre-defined test data with known IDs for deterministic tests.
type FixedTestData struct {
	ClassName string
	Objects   []*models.Object
}

// NewFixedTestData creates test data with known, fixed IDs.
func NewFixedTestData(className string) *FixedTestData {
	return &FixedTestData{
		ClassName: className,
		Objects: []*models.Object{
			{
				Class: className,
				ID:    strfmt.UUID("00000000-0000-0000-0000-000000000001"),
				Properties: map[string]interface{}{
					"title":   "First Item",
					"content": "This is the first test item.",
					"count":   1,
					"score":   10.5,
					"active":  true,
					"tags":    []string{"first", "test"},
				},
			},
			{
				Class: className,
				ID:    strfmt.UUID("00000000-0000-0000-0000-000000000002"),
				Properties: map[string]interface{}{
					"title":   "Second Item",
					"content": "This is the second test item.",
					"count":   2,
					"score":   20.5,
					"active":  false,
					"tags":    []string{"second", "test"},
				},
			},
			{
				Class: className,
				ID:    strfmt.UUID("00000000-0000-0000-0000-000000000003"),
				Properties: map[string]interface{}{
					"title":   "Third Item",
					"content": "This is the third test item.",
					"count":   3,
					"score":   30.5,
					"active":  true,
					"tags":    []string{"third", "test"},
				},
			},
		},
	}
}

// IDs returns all object IDs in the fixed test data.
func (f *FixedTestData) IDs() []strfmt.UUID {
	ids := make([]strfmt.UUID, len(f.Objects))
	for i, obj := range f.Objects {
		ids[i] = obj.ID
	}
	return ids
}
