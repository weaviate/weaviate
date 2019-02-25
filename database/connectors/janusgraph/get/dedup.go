package get

type byID map[string][]interface{}

// nestedDedup does not check whether type assertions are valid, because it
// assume that the results come from the query processor where everything has
// already been verified
func nestedDedup(orig []interface{}) []interface{} {

	idMap := byID{}
	for _, item := range orig {
		itemMap := item.(map[string]interface{})
		id := itemMap["uuid"].(string)
		idMap[id] = append(idMap[id], itemMap)
	}

	return idMap.merge()
}

func (m byID) merge() []interface{} {
	var res []interface{}

	for _, value := range m {
		merged := map[string]interface{}{}
		for _, oneMap := range value {
			merged = m.mergeMaps(merged, oneMap.(map[string]interface{}))
		}

		res = append(res, merged)
	}

	return res
}

// if a key is present in both m1 and m2, it is either over-written (primitive
// types) or appended (slice type)
func (m byID) mergeMaps(m1, m2 map[string]interface{}) map[string]interface{} {
	for k, v := range m2 {
		m1value, ok := m1[k]
		if ok {
			// this key also exists in m1
			switch m1value.(type) {
			case []interface{}:
				// cool it is a slice, we can append to it
				m1[k] = append(m1[k].([]interface{}), m2[k].([]interface{})...)
			default:
				// for any other type, let's over-write
				m1[k] = v
			}
		} else {
			m1[k] = v
		}
	}

	return m1
}
