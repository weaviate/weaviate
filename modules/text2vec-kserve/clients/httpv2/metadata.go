package httpv2

type modelMetadataResponse struct {
	Name     string       `json:"name"`
	Platform string       `json:"platform"`
	Backend  string       `json:"backend"`
	Versions []string     `json:"versions"`
	Inputs   []tensorSpec `json:"inputs"`
	Outputs  []tensorSpec `json:"outputs"`
}

type tensorSpec struct {
	Name     string   `json:"name"`
	Shape    []int32  `json:"shape"`
	Datatype DataType `json:"datatype"`
}

type modelMetadataError struct {
	Error string `json:"error"`
}
