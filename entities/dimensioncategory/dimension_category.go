package dimensioncategory


type DimensionCategory int

const (
	DimensionCategoryStandard DimensionCategory = iota
	DimensionCategoryPQ
	DimensionCategoryBQ
	DimensionCategorySQ
	DimensionCategoryRQ
)

func (c DimensionCategory) String() string {
	switch c {
	case DimensionCategoryPQ:
		return "pq"
	case DimensionCategoryBQ:
		return "bq"
	case DimensionCategorySQ:
		return "sq"
	case DimensionCategoryRQ:
		return "rq"
	default:
		return "standard"
	}
}

func NewDimensionCategoryFromString(s string) DimensionCategory {
	switch s {
	case "pq":
		return DimensionCategoryPQ
	case "bq":
		return DimensionCategoryBQ
	case "sq":
		return DimensionCategorySQ
	case "rq":
		return DimensionCategoryRQ
	default:
		return DimensionCategoryStandard
	}
}