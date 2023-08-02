package vectorizer

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/fatih/camelcase"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/moduletools"
)

func (v *Vectorizer) Object(ctx context.Context, obj *models.Object, objDiff *moduletools.ObjectDiff,
	settings ClassSettings,
) error {
	shouldVectorize, texts := getVectorizationInput(obj, objDiff, settings)

	if len(texts) == 0 {
		return nil
	}

	if !shouldVectorize {
		obj.Vector = objDiff.GetVec()
		return nil
	}

	text := strings.Join(texts, " ")

	result, err := v.client.Vectorize(ctx, text, settings.ToModuleConfig())
	if err != nil {
		return err
	}

	obj.Vector = result.Vector

	return nil
}

func getVectorizationInput(obj *models.Object, objDiff *moduletools.ObjectDiff, settings ClassSettings) (bool, []string) {
	shouldVectorize := objDiff == nil || objDiff.GetVec() == nil
	var texts []string = []string{}
	if settings.VectorizeClassName() {
		texts = append(texts, camelCaseToLower(obj.Class))
	}

	if obj.Properties != nil {
		schema := obj.Properties.(map[string]interface{})
		for _, prop := range sortStringKeys(schema) {
			if !settings.PropertyIndexed(prop) {
				continue
			}

			appended := false
			switch val := schema[prop].(type) {
			case []string:
				for _, elem := range val {
					appended = appendPropIfText(settings, &texts, prop, elem) || appended
				}
			case []interface{}:
				for _, elem := range val {
					appended = appendPropIfText(settings, &texts, prop, elem) || appended
				}
			default:
				appended = appendPropIfText(settings, &texts, prop, val)
			}

			shouldVectorize = shouldVectorize || (appended && objDiff != nil && objDiff.IsChangedProp(prop))
		}
	}
	if len(texts) == 0 {
		return false, texts
	}

	return shouldVectorize, texts
}

func sortStringKeys(schemaMap map[string]interface{}) []string {
	keys := make([]string, 0, len(schemaMap))
	for k := range schemaMap {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func appendPropIfText(icheck ClassSettings, list *[]string, propName string,
	value interface{},
) bool {
	valueString, ok := value.(string)
	if ok {
		if icheck.VectorizePropertyName(propName) {
			// use prop and value
			*list = append(*list, strings.ToLower(
				fmt.Sprintf("%s %s", camelCaseToLower(propName), valueString)))
		} else {
			*list = append(*list, strings.ToLower(valueString))
		}
		return true
	}
	return false
}

func camelCaseToLower(in string) string {
	parts := camelcase.Split(in)
	var sb strings.Builder
	for i, part := range parts {
		if part == " " {
			continue
		}

		if i > 0 {
			sb.WriteString(" ")
		}

		sb.WriteString(strings.ToLower(part))
	}

	return sb.String()
}
