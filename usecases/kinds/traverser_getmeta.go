package kinds

import (
	"crypto/md5"
	"encoding/json"
	"fmt"

	"github.com/creativesoftwarefdn/weaviate/adapters/handlers/graphql/local/common_filters"
	cf "github.com/creativesoftwarefdn/weaviate/adapters/handlers/graphql/local/common_filters"
	"github.com/creativesoftwarefdn/weaviate/entities/schema"
	"github.com/creativesoftwarefdn/weaviate/entities/schema/kind"
)

// LocalGetMeta resolves meta queries
func (t *Traverser) LocalGetMeta(params GetMetaParams) (interface{}, error) {
	unlock, err := t.locks.LockConnector()
	if err != nil {
		return nil, fmt.Errorf("could not acquire lock: %v", err)
	}
	defer unlock()

	return t.repo.LocalGetMeta(&params)
}

// GetMetaParams to describe the Local->GetMeta->Kind->Class query. Will be
// passed to the individual connector methods responsible for resolving the
// GetMeta query.
type GetMetaParams struct {
	Kind             kind.Kind
	Filters          *common_filters.LocalFilter
	Analytics        common_filters.AnalyticsProps
	ClassName        schema.ClassName
	Properties       []MetaProperty
	IncludeMetaCount bool
}

// StatisticalAnalysis is the desired computation that the database connector
// should perform on this property
type StatisticalAnalysis string

const (
	// Type can be applied to any field and will return the type of the field,
	// such as "int" or "string"
	Type StatisticalAnalysis = "type"

	// Count the occurence of this property
	Count StatisticalAnalysis = "count"

	// Sum of all the values of the prop (i.e. sum of all Ints or Numbers)
	Sum StatisticalAnalysis = "sum"

	// Mean calculates the mean of an Int or Number
	Mean StatisticalAnalysis = "mean"

	// Maximum selects the maximum value of an Int or Number
	Maximum StatisticalAnalysis = "maximum"

	// Minimum selects the maximum value of an Int or Number
	Minimum StatisticalAnalysis = "minimum"

	// TotalTrue is the sum of all boolean fields, that are true
	TotalTrue StatisticalAnalysis = "totalTrue"

	// TotalFalse is the sum of all boolean fields, that are false
	TotalFalse StatisticalAnalysis = "totalFalse"

	// PercentageTrue is the percentage of all boolean fields, that are true
	PercentageTrue StatisticalAnalysis = "percentageTrue"

	// PercentageFalse is the percentage of all boolean fields, that are false
	PercentageFalse StatisticalAnalysis = "percentageFalse"

	// PointingTo is the list of all classes that this reference prop points to
	PointingTo StatisticalAnalysis = "pointingTo"

	// TopOccurrences of strings, selection can be made more specific with
	// TopOccurrencesValues for now. In the future there might also be other
	// sub-props.
	TopOccurrences StatisticalAnalysis = "topOccurrences"

	// TopOccurrencesValue is a sub-prop of TopOccurrences
	TopOccurrencesValue StatisticalAnalysis = "value"

	// TopOccurrencesOccurs is a sub-prop of TopOccurrences
	TopOccurrencesOccurs StatisticalAnalysis = "occurs"
)

// MetaProperty is any property of a class that we want to retrieve meta
// information about
type MetaProperty struct {
	Name                schema.PropertyName
	StatisticalAnalyses []StatisticalAnalysis
}

// ParseAnalysisProp from string
func ParseAnalysisProp(name string) (StatisticalAnalysis, error) {
	switch name {
	case string(Type):
		return Type, nil
	case string(Mean):
		return Mean, nil
	case string(Maximum):
		return Maximum, nil
	case string(Minimum):
		return Minimum, nil
	case string(Count):
		return Count, nil
	case string(Sum):
		return Sum, nil
	case string(TotalTrue):
		return TotalTrue, nil
	case string(TotalFalse):
		return TotalFalse, nil
	case string(PercentageTrue):
		return PercentageTrue, nil
	case string(PercentageFalse):
		return PercentageFalse, nil
	case string(PointingTo):
		return PointingTo, nil
	case string(TopOccurrences):
		return TopOccurrences, nil
	default:
		return "", fmt.Errorf("unrecognized statistical prop '%s'", name)
	}
}

// AnalyticsHash is a special hash for use with an external analytics engine
// which has caching capabilities. Anything that would produce a different
// result, such as new or different properties or different analytics props
// will create a different hash. Chaning anayltics-meta information, such as
// 'forceRecalculate' however, will not change the hash. Doing so would prevent
// us from ever retrieving a cached result that wass generated with the
// 'forceRecalculate' option on.
func (p GetMetaParams) AnalyticsHash() (string, error) {

	// make sure to copy the params, so that we don't accidentaly mutate the
	// original
	params := p
	// always override analytical props to make sure they don't influence the
	// hash
	params.Analytics = cf.AnalyticsProps{}

	return params.md5()
}

func (p GetMetaParams) md5() (string, error) {
	paramBytes, err := json.Marshal(p)
	if err != nil {
		return "", fmt.Errorf("couldnt convert params to json before hashing: %s", err)
	}

	hash := md5.New()
	fmt.Fprintf(hash, "%s", paramBytes)
	return fmt.Sprintf("%x", hash.Sum(nil)), nil
}
