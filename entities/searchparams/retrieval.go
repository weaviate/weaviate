package searchparams

type NearVector struct {
	Vector    []float32 `json:"vector"`
	Certainty float64   `json:"certainty"`
}

type KeywordRanking struct {
	Type       string   `json:"type"`
	Properties []string `json:"properties"`
	Query      string   `json:"query"`
}

type NearObject struct {
	ID        string  `json:"id"`
	Beacon    string  `json:"beacon"`
	Certainty float64 `json:"certainty"`
}
