package vectorizer

import (
	"context"
	"fmt"
	"net/url"
	"strconv"
	"strings"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/modules/text2vec-kserve/ent"
	"github.com/weaviate/weaviate/modules/text2vec-kserve/utils"
)

const (
	DefaultOptionWaitForModel    = false
	DefaultVectorizeClassName    = false
	DefaultPropertyIndexed       = true
	DefaultVectorizePropertyName = false
)

type ClassSettings interface {
	PropertyIndexed(property string) bool
	VectorizePropertyName(propertyName string) bool
	VectorizeClassName() bool
	Url() string
	Protocol() string
	ConnectionArgs() map[string]interface{}
	ModelName() string
	ModelVersion() string
	ModelInput() string
	ModelOutput() string
	EmbeddingDims() int32
	OptionWaitForModel() bool
	ToModuleConfig() ent.ModuleConfig
}

type classSettings struct {
	cfg moduletools.ClassConfig
}

// ToModuleConfig implements ClassSettings.
func (cs *classSettings) ToModuleConfig() ent.ModuleConfig {
	return ent.ModuleConfig{
		Url:            cs.Url(),
		Protocol:       cs.Protocol(),
		Model:          cs.ModelName(),
		Version:        cs.ModelVersion(),
		Input:          cs.ModelInput(),
		Output:         cs.ModelOutput(),
		EmbeddingDims:  cs.EmbeddingDims(),
		ConnectionArgs: cs.ConnectionArgs(),
		WaitForModel:   cs.OptionWaitForModel(),
	}
}

func NewClassSettings(cfg moduletools.ClassConfig) *classSettings {
	return &classSettings{cfg: cfg}
}

func (cs *classSettings) PropertyIndexed(property string) bool {
	if cs.cfg == nil {
		// we would receive a nil-config on cross-class requests, such as Explore{}
		return DefaultPropertyIndexed
	}

	obj, ok := cs.cfg.Property(property)["skip"]
	if !ok {
		return DefaultPropertyIndexed
	}

	asBool, ok := obj.(bool)
	if !ok {
		return DefaultPropertyIndexed
	}

	return !asBool
}

func (cs *classSettings) VectorizePropertyName(propertyName string) bool {
	if cs.cfg == nil {
		// we would receive a nil-config on cross-class requests, such as Explore{}
		return DefaultVectorizePropertyName
	}
	vcn, ok := cs.cfg.Property(propertyName)["vectorizePropertyName"]
	if !ok {
		return DefaultVectorizePropertyName
	}

	asBool, ok := vcn.(bool)
	if !ok {
		return DefaultVectorizePropertyName
	}

	return asBool
}

func (cs *classSettings) VectorizeClassName() bool {
	if cs.cfg == nil {
		// we would receive a nil-config on cross-class requests, such as Explore{}
		return DefaultVectorizeClassName
	}

	vcn, ok := cs.cfg.Class()["vectorizeClassName"]
	if !ok {
		return DefaultVectorizeClassName
	}

	asBool, ok := vcn.(bool)
	if !ok {
		return DefaultVectorizeClassName
	}

	return asBool
}

func (cs *classSettings) Url() string {
	return cs.getModuleProperty("url")
}

func validateUrl(kServeService string, protocol ent.Protocol) error {
	if kServeService == "" {
		return errors.New("missing url")
	}

	if _, err := url.Parse(kServeService); err != nil {
		return fmt.Errorf("incorrect url \"%v\"", kServeService)
	}

	switch protocol {
	case ent.KSERVE_HTTP_V1:
		return validateHttpUrl(kServeService)
	case ent.KSERVE_HTTP_V2:
		return validateHttpUrl(kServeService)
	case ent.KSERVE_GRPC:
		return nil

	}

	return nil
}

func validateHttpUrl(rawUrl string) error {
	_, err := url.ParseRequestURI(rawUrl)
	if err != nil {
		return err
	}

	if !strings.HasPrefix(rawUrl, "http://") && !strings.HasPrefix(rawUrl, "https://") {
		return errors.New("missing url scheme")
	}

	return nil
}

func (cs *classSettings) Protocol() string {
	return cs.getModuleProperty("protocol")
}

func validateProtocol(protocol ent.Protocol) error {
	if protocol == "" {
		return errors.New("missing protocol")
	}

	if !utils.Contains(ent.KSERVE_VALID_PROTOCOLS, protocol) {
		return fmt.Errorf("invalid value for protocol, expected one of %v, got %v",
			strings.Join(ent.KSERVE_VALID_PROTOCOLS, ", "),
			protocol)
	}
	return nil
}

func (cs *classSettings) ConnectionArgs() map[string]interface{} {
	if args := cs.getModulePropertyRaw("connectionArgs"); args != nil {
		return args.(map[string]interface{})
	} else {
		switch cs.Protocol() {
		case ent.KSERVE_HTTP_V1:
			return ent.KSERVE_DEFAULT_HTTP_V1_CONNECTION_ARGS
		case ent.KSERVE_HTTP_V2:
			return ent.KSERVE_DEFAULT_HTTP_V2_CONNECTION_ARGS
		case ent.KSERVE_GRPC:
			return ent.KSERVE_DEFAULT_GRPC_CONNECTION_ARGS
		}
	}
	panic("ConnectionArgs was called with an invalid config, this should never happen during optimal operation as Validate should reject this config")
}

func validateConnectionArgs(kServeConnectionArgs interface{}, protocol ent.Protocol) error {
	args, isMap := kServeConnectionArgs.(map[string]interface{})
	if kServeConnectionArgs != nil && !isMap {
		return errors.New("provided connectionArgs is not a JSON map")
	}

	if args != nil {
		switch protocol {
		case ent.KSERVE_HTTP_V1:
			// TODO: validate client config
		case ent.KSERVE_HTTP_V2:
			// TODO: validate client config
		case ent.KSERVE_GRPC:
			// TODO: validate client config
		}
	}

	return nil
}

func (cs *classSettings) ModelName() string {
	return cs.getModuleProperty("modelName")
}

func (cs *classSettings) ModelVersion() string {
	return cs.getModuleProperty("modelVersion")
}

func (cs *classSettings) ModelInput() string {
	return cs.getModuleProperty("modelInput")
}

func (cs *classSettings) ModelOutput() string {
	return cs.getModuleProperty("modelOutput")
}

func (cs *classSettings) EmbeddingDims() int32 {
	raw := cs.getModulePropertyRaw("embeddingDims")
	jsonNumber := raw.(string)
	parsed, _ := strconv.Atoi(jsonNumber)
	return int32(parsed)
}

func (cs *classSettings) validateEmbeddingDims() error {
	raw := cs.getModulePropertyRaw("embeddingDims")
	if raw == nil {
		return errors.New("missing embeddingDims")
	}
	asString, ok := raw.(string)
	if !ok {
		return errors.New("embeddingDims parsing failed")
	}
	parsed, err := strconv.Atoi(asString)
	if err != nil {
		return err
	}

	if parsed <= 0 {
		return fmt.Errorf("embeddingDims %v is not a valid positive integer", parsed)
	}

	return nil
}

func (cs *classSettings) OptionWaitForModel() bool {
	return cs.getModuleOptionOrDefault("waitForModel", DefaultOptionWaitForModel)
}

func (cs *classSettings) Validate(ctx context.Context, class *models.Class) error {
	if cs.cfg == nil {
		// we would receive a nil-config on cross-class requests, such as Explore{}
		return errors.New("empty config")
	}

	validationErrors := []error{}
	numErrors := 0

	if err := cs.validateModuleConfig(ctx); err != nil {
		validationErrors = append(validationErrors, err)
		numErrors++
	}

	if err := validateIndexState(class, cs); err != nil {
		validationErrors = append(validationErrors, err)
		numErrors++
	}

	if numErrors > 0 {
		joined := utils.Join(validationErrors...)
		return errors.Wrap(joined, "validation errors")
	}

	return nil
}

func (cs *classSettings) validateModuleConfig(ctx context.Context) error {
	protocol := cs.Protocol()
	if err := validateProtocol(protocol); err != nil {
		return errors.Wrap(err, "invalid protocol")
	}

	if err := validateUrl(cs.Url(), cs.Protocol()); err != nil {
		return errors.Wrap(err, "invalid url")
	}

	if err := validateConnectionArgs(cs.ConnectionArgs(), cs.Protocol()); err != nil {
		return errors.Wrap(err, "invalid connectionArgs")
	}

	model := cs.ModelName()
	if model == "" {
		return errors.New("missing modelName")
	}

	modelInput := cs.ModelInput()
	if modelInput == "" {
		return errors.New("missing modelInput")
	}

	modelOutput := cs.ModelOutput()
	if modelOutput == "" {
		return errors.New("missing modelOutput")
	}

	if err := cs.validateEmbeddingDims(); err != nil {
		return err
	}

	return nil
}

func (cs *classSettings) getModuleProperty(name string) string {
	if cs.cfg != nil {
		model, ok := cs.cfg.Class()[name]
		if ok {
			asString, ok := model.(string)
			if ok {
				return asString
			}
		}
	}
	return ""
}

func (cs *classSettings) getModulePropertyRaw(name string) interface{} {
	if cs.cfg != nil {
		prop, ok := cs.cfg.Class()[name]
		if ok {
			return prop
		}
	}
	return nil
}

func (cs *classSettings) getModuleOptionOrDefault(option string, defaultValue bool) bool {
	if value := cs.getModuleOption(option); value != nil {
		return *value
	}
	return defaultValue
}

func (cs *classSettings) getModuleOption(option string) *bool {
	if cs.cfg != nil {
		options, ok := cs.cfg.Class()["options"]
		if ok {
			asMap, ok := options.(map[string]interface{})
			if ok {
				option, ok := asMap[option]
				if ok {
					asBool, ok := option.(bool)
					if ok {
						return &asBool
					}
				}
			}
		}
	}
	return nil
}

func validateIndexState(class *models.Class, settings ClassSettings) error {
	if settings.VectorizeClassName() {
		// if the user chooses to vectorize the classname, vector-building will
		// always be possible, no need to investigate further

		return nil
	}

	// search if there is at least one indexed, string/text prop. If found pass
	// validation
	for _, prop := range class.Properties {
		if len(prop.DataType) < 1 {
			return errors.Errorf("property %s must have at least one datatype: "+
				"got %v", prop.Name, prop.DataType)
		}

		if prop.DataType[0] != string(schema.DataTypeText) {
			// we can only vectorize text-like props
			continue
		}

		if settings.PropertyIndexed(prop.Name) {
			// found at least one, this is a valid schema
			return nil
		}
	}

	return fmt.Errorf("invalid properties: didn't find a single property which is " +
		"of type string or text and is not excluded from indexing. In addition the " +
		"class name is excluded from vectorization as well, meaning that it cannot be " +
		"used to determine the vector position. To fix this, set 'vectorizeClassName' " +
		"to true if the class name is contextionary-valid. Alternatively add at least " +
		"contextionary-valid text/string property which is not excluded from " +
		"indexing")
}
