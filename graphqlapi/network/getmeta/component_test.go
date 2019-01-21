package getmeta

// Removed until we have refactored the getmeta package

// type testCase struct {
// 	name            string
// 	query           string
// 	resolverReturn  interface{}
// 	expectedResults []result
// }

// type testCases []testCase

// type result struct {
// 	pathToField   []string
// 	expectedValue interface{}
// }

// func TestNetworkGetMeta(t *testing.T) {

// }

// func (tests testCases) Assert(t *testing.T, k kind.Kind, className string) {
// 	for _, testCase := range tests {
// 		t.Run(testCase.name, func(t *testing.T) {
// 			resolver := newMockResolver()

// 			resolverReturn := &models.GraphQLResponse{
// 				Data: map[string]models.JSONObject{
// 					"Local": map[string]interface{}{
// 						"GetMeta": testCase.resolverReturn,
// 					},
// 				},
// 			}

// 			resolver.On("LocalGetMeta").
// 				Return(resolverReturn, nil).Once()

// 			result := resolver.AssertResolve(t, testCase.query)

// 			for _, expectedResult := range testCase.expectedResults {
// 				value := result.Get(expectedResult.pathToField...).Result

// 				assert.Equal(t, expectedResult.expectedValue, value)
// 			}
// 		})
// 	}
// }

// type fakeNetworkResolver struct {
// 	returnValue interface{}
// }

// func (r *fakeNetworkResolver) ProxyGetMetaInstance(info Params) (*models.GraphQLResponse, error) {
// 	return &models.GraphQLResponse{
// 		Data: map[string]models.JSONObject{
// 			"Local": map[string]interface{}{
// 				"GetMeta": r.returnValue,
// 			},
// 		},
// 	}, nil
// }

// type mockResolver struct {
// 	helper.MockResolver
// }

// func newMockResolver() *mockResolver {
// 	field, err := Build(&testhelper.CarSchema)
// 	if err != nil {
// 		panic(fmt.Sprintf("could not build graphql test schema: %s", err))
// 	}
// 	mocker := &mockResolver{}
// 	mocker.RootFieldName = "GetMeta"
// 	mocker.RootField = field
// 	mocker.RootObject = map[string]interface{}{"Resolver": Resolver(mocker)}
// 	return mocker
// }

// func (m *mockResolver) LocalGetMeta(params *Params) (interface{}, error) {
// 	args := m.Called(params)
// 	return args.Get(0), args.Error(1)
// }
