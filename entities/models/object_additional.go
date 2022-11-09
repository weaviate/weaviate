package models

func (m *Object) GetAdditionalProperty(name string) interface{} {
	return m.Properties.(map[string]interface{})[name]
}

func (m *Object) SetAdditionalProperty(name string, data interface{})  {
	m.Properties.(map[string]interface{})[name]=data
}
