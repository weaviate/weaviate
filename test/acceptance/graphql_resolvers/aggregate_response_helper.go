package test

import (
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/semi-technologies/weaviate/entities/schema"
)

type gqlAggregateResponseHelper struct{}

func (h *gqlAggregateResponseHelper) boolean(count, totalFalse, totalTrue int64,
	percentageFalse, percentageTrue float64,
) map[string]interface{} {
	return map[string]interface{}{
		"count":           json.Number(fmt.Sprint(count)),
		"percentageFalse": json.Number(printFloatNoScientific(percentageFalse)),
		"percentageTrue":  json.Number(printFloatNoScientific(percentageTrue)),
		"totalFalse":      json.Number(fmt.Sprint(totalFalse)),
		"totalTrue":       json.Number(fmt.Sprint(totalTrue)),
		"type":            string(schema.DataTypeBoolean),
	}
}

func (h *gqlAggregateResponseHelper) booleans(count, totalFalse, totalTrue int64,
	percentageFalse, percentageTrue float64,
) map[string]interface{} {
	b := h.boolean(count, totalFalse, totalTrue, percentageFalse, percentageTrue)
	b["type"] = string(schema.DataTypeBooleanArray)
	return b
}

func (h *gqlAggregateResponseHelper) boolean0() map[string]interface{} {
	return map[string]interface{}{
		"count":           json.Number("0"),
		"percentageFalse": nil,
		"percentageTrue":  nil,
		"totalFalse":      json.Number("0"),
		"totalTrue":       json.Number("0"),
		"type":            string(schema.DataTypeBoolean),
	}
}

func (h *gqlAggregateResponseHelper) booleans0() map[string]interface{} {
	b := h.boolean0()
	b["type"] = string(schema.DataTypeBooleanArray)
	return b
}

func (h *gqlAggregateResponseHelper) int(count, maximum, minimum, mode, sum int64,
	median, mean float64,
) map[string]interface{} {
	return map[string]interface{}{
		"count":   json.Number(fmt.Sprint(count)),
		"maximum": json.Number(fmt.Sprint(maximum)),
		"mean":    json.Number(printFloatNoScientific(mean)),
		"median":  json.Number(printFloatNoScientific(median)),
		"minimum": json.Number(fmt.Sprint(minimum)),
		"mode":    json.Number(fmt.Sprint(mode)),
		"sum":     json.Number(fmt.Sprint(sum)),
		"type":    string(schema.DataTypeInt),
	}
}

func (h *gqlAggregateResponseHelper) ints(count, maximum, minimum, mode, sum int64,
	median, mean float64,
) map[string]interface{} {
	i := h.int(count, maximum, minimum, mode, sum, median, mean)
	i["type"] = string(schema.DataTypeIntArray)
	return i
}

func (h *gqlAggregateResponseHelper) int0() map[string]interface{} {
	return map[string]interface{}{
		"count":   json.Number("0"),
		"maximum": nil,
		"mean":    nil,
		"median":  nil,
		"minimum": nil,
		"mode":    nil,
		"sum":     nil,
		"type":    string(schema.DataTypeInt),
	}
}

func (h *gqlAggregateResponseHelper) ints0() map[string]interface{} {
	i := h.int0()
	i["type"] = string(schema.DataTypeIntArray)
	return i
}

func (h *gqlAggregateResponseHelper) number(count int64,
	maximum, minimum, mode, sum, median, mean float64,
) map[string]interface{} {
	return map[string]interface{}{
		"count":   json.Number(fmt.Sprint(count)),
		"maximum": json.Number(printFloatNoScientific(maximum)),
		"mean":    json.Number(printFloatNoScientific(mean)),
		"median":  json.Number(printFloatNoScientific(median)),
		"minimum": json.Number(printFloatNoScientific(minimum)),
		"mode":    json.Number(printFloatNoScientific(mode)),
		"sum":     json.Number(printFloatNoScientific(sum)),
		"type":    string(schema.DataTypeNumber),
	}
}

func (h *gqlAggregateResponseHelper) numbers(count int64,
	maximum, minimum, mode, sum, median, mean float64,
) map[string]interface{} {
	n := h.number(count, maximum, minimum, mode, sum, median, mean)
	n["type"] = string(schema.DataTypeNumberArray)
	return n
}

func (h *gqlAggregateResponseHelper) number0() map[string]interface{} {
	return map[string]interface{}{
		"count":   json.Number("0"),
		"maximum": nil,
		"mean":    nil,
		"median":  nil,
		"minimum": nil,
		"mode":    nil,
		"sum":     nil,
		"type":    string(schema.DataTypeNumber),
	}
}

func (h *gqlAggregateResponseHelper) numbers0() map[string]interface{} {
	n := h.number0()
	n["type"] = string(schema.DataTypeNumberArray)
	return n
}

func (h *gqlAggregateResponseHelper) string(count int64, values []string, occurrences []int64,
) map[string]interface{} {
	to := make([]interface{}, len(values))
	for i := range values {
		to[i] = map[string]interface{}{
			"occurs": json.Number(fmt.Sprint(occurrences[i])),
			"value":  values[i],
		}
	}

	return map[string]interface{}{
		"count":          json.Number(fmt.Sprint(count)),
		"topOccurrences": to,
		"type":           string(schema.DataTypeString),
	}
}

func (h *gqlAggregateResponseHelper) strings(count int64, values []string, occurrences []int64,
) map[string]interface{} {
	s := h.string(count, values, occurrences)
	s["type"] = string(schema.DataTypeStringArray)
	return s
}

func (h *gqlAggregateResponseHelper) string0() map[string]interface{} {
	return map[string]interface{}{
		"count":          json.Number("0"),
		"topOccurrences": []interface{}{},
		"type":           string(schema.DataTypeString),
	}
}

func (h *gqlAggregateResponseHelper) strings0() map[string]interface{} {
	s := h.string0()
	s["type"] = string(schema.DataTypeStringArray)
	return s
}

func (h *gqlAggregateResponseHelper) text(count int64, values []string, occurrences []int64,
) map[string]interface{} {
	to := make([]interface{}, len(values))
	for i := range values {
		to[i] = map[string]interface{}{
			"occurs": json.Number(fmt.Sprint(occurrences[i])),
			"value":  values[i],
		}
	}

	return map[string]interface{}{
		"count":          json.Number(fmt.Sprint(count)),
		"topOccurrences": to,
		"type":           string(schema.DataTypeText),
	}
}

func (h *gqlAggregateResponseHelper) texts(count int64, values []string, occurrences []int64,
) map[string]interface{} {
	t := h.text(count, values, occurrences)
	t["type"] = string(schema.DataTypeTextArray)
	return t
}

func (h *gqlAggregateResponseHelper) text0() map[string]interface{} {
	return map[string]interface{}{
		"count":          json.Number("0"),
		"topOccurrences": []interface{}{},
		"type":           string(schema.DataTypeText),
	}
}

func (h *gqlAggregateResponseHelper) texts0() map[string]interface{} {
	t := h.text0()
	t["type"] = string(schema.DataTypeTextArray)
	return t
}

func (h *gqlAggregateResponseHelper) date(count int64) map[string]interface{} {
	return map[string]interface{}{
		"count": json.Number(fmt.Sprint(count)),
	}
}

func (h *gqlAggregateResponseHelper) dates(count int64) map[string]interface{} {
	return h.date(count)
}

func (h *gqlAggregateResponseHelper) date0() map[string]interface{} {
	return h.date(0)
}

func (h *gqlAggregateResponseHelper) dates0() map[string]interface{} {
	return h.dates(0)
}

func (h *gqlAggregateResponseHelper) meta(count int64) map[string]interface{} {
	return map[string]interface{}{
		"count": json.Number(fmt.Sprint(count)),
	}
}

func (h *gqlAggregateResponseHelper) groupedBy(value string, path ...interface{}) map[string]interface{} {
	return map[string]interface{}{
		"value": value,
		"path":  path,
	}
}

func (h *gqlAggregateResponseHelper) pointingTo(path ...interface{}) map[string]interface{} {
	return map[string]interface{}{
		"pointingTo": path,
		"type":       string(schema.DataTypeCRef),
	}
}

func printFloatNoScientific(f float64) string {
	return strconv.FormatFloat(f, 'f', -1, 64)
}
