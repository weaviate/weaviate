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

package schema

import (
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"strings"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/modelsext"
	"github.com/weaviate/weaviate/entities/schema"
	schemaConfig "github.com/weaviate/weaviate/entities/schema/config"
	"github.com/weaviate/weaviate/entities/vectorindex"
	"github.com/weaviate/weaviate/usecases/config"
	shardingConfig "github.com/weaviate/weaviate/usecases/sharding/config"
)

var errPropertiesUpdatedInClassUpdate = errors.Errorf(
	"property fields other than description cannot be updated through updating the class. Use the add " +
		"property feature (e.g. \"POST /v1/schema/{className}/properties\") " +
		"to add additional properties")

type modulesProvider interface {
	IsGenerative(string) bool
	IsReranker(string) bool
	IsMultiVector(string) bool
}

type Parser struct {
	clusterState clusterState
	configParser VectorConfigParser
	validator    validator
	modules      modulesProvider
}

func NewParser(cs clusterState, vCfg VectorConfigParser, v validator, modules modulesProvider) *Parser {
	return &Parser{
		clusterState: cs,
		configParser: vCfg,
		validator:    v,
		modules:      modules,
	}
}

func (p *Parser) ParseClass(class *models.Class) error {
	if class == nil {
		return fmt.Errorf("class cannot be nil")
	}

	if strings.EqualFold(class.Class, config.DefaultRaftDir) {
		return fmt.Errorf("parse class name: %w", fmt.Errorf("class name `raft` is reserved"))
	}

	if err := p.parseShardingConfig(class); err != nil {
		return fmt.Errorf("parse sharding config: %w", err)
	}

	if err := p.parseVectorIndexConfig(class); err != nil {
		return fmt.Errorf("parse vector index config: %w", err)
	}

	return nil
}

func (p *Parser) parseModuleConfig(class *models.Class) error {
	if class.ModuleConfig == nil {
		return nil
	}

	mapMC, ok := class.ModuleConfig.(map[string]any)
	if !ok {
		return fmt.Errorf("module config is not a map, got %v", class.ModuleConfig)
	}

	mc, err := p.moduleConfig(mapMC)
	if err != nil {
		return fmt.Errorf("module config: %w", err)
	}
	class.ModuleConfig = mc

	return nil
}

func (p *Parser) parseVectorConfig(class *models.Class) error {
	if class.VectorConfig == nil {
		return nil
	}

	newVC := map[string]models.VectorConfig{}
	for vector, config := range class.VectorConfig {
		mapMC, ok := config.Vectorizer.(map[string]any)
		if !ok {
			return fmt.Errorf("vectorizer for %s is not a map, got %v", vector, config)
		}

		mc, err := p.moduleConfig(mapMC)
		if err != nil {
			return fmt.Errorf("vectorizer config: %w", err)
		}

		config.Vectorizer = mc
		newVC[vector] = config
	}
	class.VectorConfig = newVC
	return nil
}

func (p *Parser) moduleConfig(moduleConfig map[string]any) (map[string]any, error) {
	parsedMC := map[string]any{}
	for module, config := range moduleConfig {
		if config == nil {
			parsedMC[module] = nil // nil is allowed, do no further parsing
			continue
		}
		mapC, ok := config.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("module config for %s is not a map, got %v", module, config)
		}
		parsedC := map[string]any{}
		// raft interprets all `json.Number` types as float64 when unmarshalling
		// we parse them explicitly here so that UpdateClass can compare the new class
		// with the old one read from the raft schema manager
		for key, value := range mapC {
			if number, ok := value.(json.Number); ok {
				if integer, err := number.Int64(); err == nil {
					parsedC[key] = float64(integer)
				} else if float, err := number.Float64(); err == nil {
					parsedC[key] = float
				} else {
					parsedC[key] = number.String()
				}
				continue
			}
			parsedC[key] = value
		}
		parsedMC[module] = parsedC
	}
	return parsedMC, nil
}

func (p *Parser) parseVectorIndexConfig(class *models.Class) error {
	if !hasTargetVectors(class) || class.VectorIndexType != "" {
		parsed, err := p.parseGivenVectorIndexConfig(class.VectorIndexType, class.VectorIndexConfig, p.modules.IsMultiVector(class.Vectorizer))
		if err != nil {
			return err
		}
		if parsed.IsMultiVector() {
			return fmt.Errorf("class.VectorIndexConfig multi vector type index type is only configurable using named vectors")
		}
		class.VectorIndexConfig = parsed
	}

	if err := p.parseTargetVectorsIndexConfig(class); err != nil {
		return err
	}
	return nil
}

func (p *Parser) parseShardingConfig(class *models.Class) (err error) {
	// multiTenancyConfig and shardingConfig are mutually exclusive
	cfg := shardingConfig.Config{} // cfg is empty in case of MT
	if !schema.MultiTenancyEnabled(class) {
		cfg, err = shardingConfig.ParseConfig(class.ShardingConfig, p.clusterState.NodeCount())
		if err != nil {
			return err
		}

	}
	class.ShardingConfig = cfg
	return nil
}

func (p *Parser) parseTargetVectorsIndexConfig(class *models.Class) error {
	for targetVector, vectorConfig := range class.VectorConfig {
		isMultiVector := false
		vectorizerModuleName := ""
		if vectorizer, ok := vectorConfig.Vectorizer.(map[string]interface{}); ok {
			for name := range vectorizer {
				isMultiVector = p.modules.IsMultiVector(name)
				vectorizerModuleName = name
			}
		}
		parsed, err := p.parseGivenVectorIndexConfig(vectorConfig.VectorIndexType, vectorConfig.VectorIndexConfig, isMultiVector)
		if err != nil {
			return fmt.Errorf("parse vector config for %s: %w", targetVector, err)
		}
		if parsed.IsMultiVector() && vectorizerModuleName != "none" && !isMultiVector {
			return fmt.Errorf("parse vector config for %s: multi vector index configured but vectorizer: %q doesn't support multi vectors", targetVector, vectorizerModuleName)
		}
		vectorConfig.VectorIndexConfig = parsed
		class.VectorConfig[targetVector] = vectorConfig
	}
	return nil
}

func (p *Parser) parseGivenVectorIndexConfig(vectorIndexType string,
	vectorIndexConfig interface{}, isMultiVector bool,
) (schemaConfig.VectorIndexConfig, error) {
	if vectorIndexType != vectorindex.VectorIndexTypeHNSW && vectorIndexType != vectorindex.VectorIndexTypeFLAT && vectorIndexType != vectorindex.VectorIndexTypeDYNAMIC {
		return nil, errors.Errorf(
			"parse vector index config: unsupported vector index type: %q",
			vectorIndexType)
	}

	if vectorIndexType != vectorindex.VectorIndexTypeHNSW && isMultiVector {
		return nil, errors.Errorf(
			"parse vector index config: multi vector index is not supported for vector index type: %q, only supported type is hnsw",
			vectorIndexType)
	}

	parsed, err := p.configParser(vectorIndexConfig, vectorIndexType, isMultiVector)
	if err != nil {
		return nil, errors.Wrap(err, "parse vector index config")
	}
	return parsed, nil
}

// ParseClassUpdate parses a class after unmarshaling by setting concrete types for the fields
func (p *Parser) ParseClassUpdate(class, update *models.Class) (*models.Class, error) {
	if err := p.ParseClass(update); err != nil {
		return nil, err
	}
	mtEnabled, err := validateUpdatingMT(class, update)
	if err != nil {
		return nil, err
	}

	if err := validateImmutableFields(class, update); err != nil {
		return nil, err
	}

	if err := p.validateModuleConfigsParityAndImmutables(class, update); err != nil {
		return nil, err
	}

	// run target vectors validation first, as it will reject classes
	// where legacy vector was changed to target vectors and vice versa
	if err = p.validateNamedVectorConfigsParityAndImmutables(class, update); err != nil {
		return nil, err
	}

	if err = validateLegacyVectorIndexConfigImmutableFields(class, update); err != nil {
		return nil, err
	}

	if class.VectorIndexConfig != nil || update.VectorIndexConfig != nil {
		vIdxConfig, ok1 := class.VectorIndexConfig.(schemaConfig.VectorIndexConfig)
		vIdxConfigU, ok2 := update.VectorIndexConfig.(schemaConfig.VectorIndexConfig)
		if !ok1 || !ok2 {
			return nil, fmt.Errorf("vector index config wrong type: current=%t new=%t", ok1, ok2)
		}
		if err := p.validator.ValidateVectorIndexConfigUpdate(vIdxConfig, vIdxConfigU); err != nil {
			return nil, fmt.Errorf("validate vector index config: %w", err)
		}
	}

	if hasTargetVectors(update) {
		if err := p.validator.ValidateVectorIndexConfigsUpdate(
			asVectorIndexConfigs(class), asVectorIndexConfigs(update)); err != nil {
			return nil, err
		}
	}

	if err := validateShardingConfig(class, update, mtEnabled); err != nil {
		return nil, fmt.Errorf("validate sharding config: %w", err)
	}

	if err = p.validatePropertiesForUpdate(class.Properties, update.Properties); err != nil {
		return nil, err
	}

	if err := p.validator.ValidateInvertedIndexConfigUpdate(
		class.InvertedIndexConfig,
		update.InvertedIndexConfig); err != nil {
		return nil, fmt.Errorf("inverted index config: %w", err)
	}

	return update, nil
}

func (p *Parser) validatePropertiesForUpdate(existing []*models.Property, new []*models.Property) error {
	if len(existing) != len(new) {
		return errPropertiesUpdatedInClassUpdate
	}

	sort.Slice(existing, func(i, j int) bool {
		return existing[i].Name < existing[j].Name
	})

	sort.Slice(new, func(i, j int) bool {
		return new[i].Name < new[j].Name
	})

	for i, prop := range existing {
		// make a copy of the properties to remove the description field
		// so that we can compare the rest of the fields
		if prop == nil {
			continue
		}
		if new[i] == nil {
			continue
		}

		if err := p.validatePropertyForUpdate(prop, new[i]); err != nil {
			return errors.Wrapf(err, "property %q", prop.Name)
		}
	}

	return nil
}

func propertyAsMap(in any) (map[string]any, error) {
	out := make(map[string]any)

	v := reflect.ValueOf(in)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}

	if v.Kind() != reflect.Struct { // Non-structural return error
		return nil, fmt.Errorf("asMap only accepts struct or struct pointer; got %T", v)
	}

	t := v.Type()
	// Traversing structure fields
	// Specify the tagName value as the key in the map; the field value as the value in the map
	for i := 0; i < v.NumField(); i++ {
		tfi := t.Field(i)
		if tagValue := tfi.Tag.Get("json"); tagValue != "" {
			key := strings.Split(tagValue, ",")[0]
			if key == "description" {
				continue
			}
			if key == "nestedProperties" {
				nps := v.Field(i).Interface().([]*models.NestedProperty)
				out[key] = make([]map[string]any, 0, len(nps))
				for _, np := range nps {
					npm, err := propertyAsMap(np)
					if err != nil {
						return nil, err
					}
					out[key] = append(out[key].([]map[string]any), npm)
				}
				continue
			}
			out[key] = v.Field(i).Interface()
		}
	}
	return out, nil
}

func (p *Parser) validatePropertyForUpdate(existing, new *models.Property) error {
	e, err := propertyAsMap(existing)
	if err != nil {
		return errors.Wrap(err, "converting existing properties to a map")
	}

	n, err := propertyAsMap(new)
	if err != nil {
		return errors.Wrap(err, "converting new properties to a map")
	}

	var (
		existingModuleConfig = cutModuleConfig(e)
		newModuleConfig      = cutModuleConfig(n)
	)

	for moduleName, existingCfg := range existingModuleConfig {
		newCfg, ok := newModuleConfig[moduleName]
		if !ok {
			return errors.Errorf("module %q configuration was removed", moduleName)
		}

		if !reflect.DeepEqual(existingCfg, newCfg) {
			return errors.Errorf("module %q configuration cannot be updated", moduleName)
		}
	}

	if !reflect.DeepEqual(e, n) {
		return errPropertiesUpdatedInClassUpdate
	}

	return nil
}

func cutModuleConfig(properties map[string]any) map[string]any {
	cfg, _ := properties["moduleConfig"].(map[string]any)
	delete(properties, "moduleConfig")
	return cfg
}

func hasTargetVectors(class *models.Class) bool {
	return len(class.VectorConfig) > 0
}

func (p *Parser) validateModuleConfigsParityAndImmutables(initial, updated *models.Class) error {
	if updated.ModuleConfig == nil || reflect.DeepEqual(initial.ModuleConfig, updated.ModuleConfig) {
		return nil
	}

	updatedModConf, ok := updated.ModuleConfig.(map[string]any)
	if !ok {
		return fmt.Errorf("module config for %s is not a map, got %v", updated.ModuleConfig, updated.ModuleConfig)
	}

	updatedModConf, err := p.moduleConfig(updatedModConf)
	if err != nil {
		return err
	}

	initialModConf, _ := initial.ModuleConfig.(map[string]any)

	// this part:
	// - allow adding new modules
	// - only allows updating generative and rerankers
	// - only one gen/rerank module can be present. Existing ones will be replaced, updating with more than one is not
	//   allowed
	// - other modules will not be changed. They can be present in the update if they have EXACTLY the same settings
	hasGenerativeUpdate := false
	hasRerankerUpdate := false
	for module := range updatedModConf {
		if p.modules.IsGenerative(module) {
			if hasGenerativeUpdate {
				return fmt.Errorf("updated moduleconfig has multiple generative modules: %v", updatedModConf)
			}
			hasGenerativeUpdate = true
			continue
		}

		if p.modules.IsReranker(module) {
			if hasRerankerUpdate {
				return fmt.Errorf("updated moduleconfig has multiple reranker modules: %v", updatedModConf)
			}
			hasRerankerUpdate = true
			continue
		}

		if _, moduleExisted := initialModConf[module]; !moduleExisted {
			continue
		}

		if reflect.DeepEqual(initialModConf[module], updatedModConf[module]) {
			continue
		}

		return fmt.Errorf("can only update generative and reranker module configs. Got: %v", module)
	}

	if initial.ModuleConfig == nil {
		initial.ModuleConfig = updatedModConf
		return nil
	}

	if _, ok := initial.ModuleConfig.(map[string]any); !ok {
		initial.ModuleConfig = updatedModConf
		return nil
	}
	if hasGenerativeUpdate {
		// clear out old generative module
		for module := range initialModConf {
			if p.modules.IsGenerative(module) {
				delete(initialModConf, module)
			}
		}
	}

	if hasRerankerUpdate {
		// clear out old reranker module
		for module := range initialModConf {
			if p.modules.IsReranker(module) {
				delete(initialModConf, module)
			}
		}
	}

	for module := range updatedModConf {
		initialModConf[module] = updatedModConf[module]
	}
	updated.ModuleConfig = initialModConf
	return nil
}

func (p *Parser) validateNamedVectorConfigsParityAndImmutables(initial, updated *models.Class) error {
	if modelsext.ClassHasLegacyVectorIndex(initial) {
		for targetVector := range updated.VectorConfig {
			if targetVector == modelsext.DefaultNamedVectorName {
				return fmt.Errorf("vector named %s cannot be created when collection level vector index is configured", modelsext.DefaultNamedVectorName)
			}
		}
	}

	for vecName, initialCfg := range initial.VectorConfig {
		updatedCfg, ok := updated.VectorConfig[vecName]
		if !ok {
			return fmt.Errorf("missing config for vector %q", vecName)
		}

		// immutable vector type
		if initialCfg.VectorIndexType != updatedCfg.VectorIndexType {
			return fmt.Errorf("vector index type of vector %q is immutable: attempted change from %q to %q",
				vecName, initialCfg.VectorIndexType, updatedCfg.VectorIndexType)
		}

		// immutable vectorizer
		if imap, ok := initialCfg.Vectorizer.(map[string]interface{}); ok && len(imap) == 1 {
			umap, ok := updatedCfg.Vectorizer.(map[string]interface{})
			if !ok || len(umap) != 1 {
				return fmt.Errorf("invalid vectorizer config for vector %q", vecName)
			}

			ivectorizer := ""
			for k := range imap {
				ivectorizer = k
			}
			uvectorizer := ""
			for k := range umap {
				uvectorizer = k
			}

			if ivectorizer != uvectorizer {
				return fmt.Errorf("vectorizer of vector %q is immutable: attempted change from %q to %q",
					vecName, ivectorizer, uvectorizer)
			}
		}
	}
	return nil
}

func asVectorIndexConfigs(c *models.Class) map[string]schemaConfig.VectorIndexConfig {
	if c.VectorConfig == nil {
		return nil
	}

	cfgs := map[string]schemaConfig.VectorIndexConfig{}
	for vecName := range c.VectorConfig {
		cfgs[vecName] = c.VectorConfig[vecName].VectorIndexConfig.(schemaConfig.VectorIndexConfig)
	}
	return cfgs
}

func validateShardingConfig(current, update *models.Class, mtEnabled bool) error {
	if mtEnabled {
		return nil
	}
	first, ok := current.ShardingConfig.(shardingConfig.Config)
	if !ok {
		return fmt.Errorf("current config is not well-formed")
	}
	second, ok := update.ShardingConfig.(shardingConfig.Config)
	if !ok {
		return fmt.Errorf("updated config is not well-formed")
	}
	if first.DesiredCount != second.DesiredCount {
		return fmt.Errorf("re-sharding not supported yet: shard count is immutable: "+
			"attempted change from \"%d\" to \"%d\"", first.DesiredCount,
			second.DesiredCount)
	}

	if first.VirtualPerPhysical != second.VirtualPerPhysical {
		return fmt.Errorf("virtual shards per physical is immutable: "+
			"attempted change from \"%d\" to \"%d\"", first.VirtualPerPhysical,
			second.VirtualPerPhysical)
	}
	return nil
}
