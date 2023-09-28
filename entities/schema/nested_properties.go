package schema

import "github.com/weaviate/weaviate/entities/models"

// merges nPropsExt with nPropsBase
// returns new slice without changing input ones
// and bool indicating whether there was changes done comparing to base slice
func MergeRecursivelyNestedProperties(nPropsBase, nPropsExt []*models.NestedProperty,
) ([]*models.NestedProperty, bool) {
	merged := false
	nProps := make([]*models.NestedProperty, len(nPropsBase), len(nPropsBase)+len(nPropsExt))
	copy(nProps, nPropsBase)

	existingIndexMap := map[string]int{}
	for index := range nProps {
		existingIndexMap[nProps[index].Name] = index
	}

	for _, nProp := range nPropsExt {
		index, exists := existingIndexMap[nProp.Name]
		if !exists {
			existingIndexMap[nProp.Name] = len(nProps)
			nProps = append(nProps, nProp)
			merged = true
		} else if _, isNested := AsNested(nProps[index].DataType); isNested {
			if mergedProps, mergedNested := MergeRecursivelyNestedProperties(nProps[index].NestedProperties,
				nProp.NestedProperties,
			); mergedNested {
				nPropCopy := *nProps[index]
				nProps[index] = &nPropCopy
				nProps[index].NestedProperties = mergedProps
				merged = true
			}
		}
	}

	return nProps, merged
}
