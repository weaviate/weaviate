package ent

type ModuleConfig struct {
	Url            string
	Protocol       Protocol
	Model          string
	Version        string
	Input          string
	Output         string
	EmbeddingDims  int32
	ConnectionArgs map[string]interface{}
	WaitForModel   bool
}
