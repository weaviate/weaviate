package vectorizer

type fakeClassConfig struct {
	classConfig map[string]interface{}
	modules     map[string]interface{}
}

func (f fakeClassConfig) Class() map[string]interface{} {
	return f.classConfig
}

func (f fakeClassConfig) ClassByModuleName(moduleName string) map[string]interface{} {
	res := f.modules[moduleName]
	return res.(map[string]interface{})
}

func (f fakeClassConfig) Property(propName string) map[string]interface{} {
	return nil
}

func (f fakeClassConfig) Tenant() string {
	return ""
}
