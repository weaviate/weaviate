//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package modbind

import (
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/usecases/modulecomponents/arguments/nearAudio"
	"github.com/weaviate/weaviate/usecases/modulecomponents/arguments/nearDepth"
	"github.com/weaviate/weaviate/usecases/modulecomponents/arguments/nearImage"
	"github.com/weaviate/weaviate/usecases/modulecomponents/arguments/nearImu"
	"github.com/weaviate/weaviate/usecases/modulecomponents/arguments/nearText"
	"github.com/weaviate/weaviate/usecases/modulecomponents/arguments/nearThermal"
	"github.com/weaviate/weaviate/usecases/modulecomponents/arguments/nearVideo"
)

func (m *BindModule) initNearText() error {
	m.nearTextSearcher = nearText.NewSearcher(m.textVectorizer)
	m.nearTextGraphqlProvider = nearText.New(m.nearTextTransformer)
	return nil
}

func (m *BindModule) initNearImage() error {
	m.nearImageSearcher = nearImage.NewSearcher(m.bindVectorizer)
	m.nearImageGraphqlProvider = nearImage.New()
	return nil
}

func (m *BindModule) initNearAudio() error {
	m.nearAudioSearcher = nearAudio.NewSearcher(m.bindVectorizer)
	m.nearAudioGraphqlProvider = nearAudio.New()
	return nil
}

func (m *BindModule) initNearVideo() error {
	m.nearVideoSearcher = nearVideo.NewSearcher(m.bindVectorizer)
	m.nearVideoGraphqlProvider = nearVideo.New()
	return nil
}

func (m *BindModule) initNearIMU() error {
	m.nearIMUSearcher = nearImu.NewSearcher(m.bindVectorizer)
	m.nearIMUGraphqlProvider = nearImu.New()
	return nil
}

func (m *BindModule) initNearThermal() error {
	m.nearThermalSearcher = nearThermal.NewSearcher(m.bindVectorizer)
	m.nearThermalGraphqlProvider = nearThermal.New()
	return nil
}

func (m *BindModule) initNearDepth() error {
	m.nearDepthSearcher = nearDepth.NewSearcher(m.bindVectorizer)
	m.nearDepthGraphqlProvider = nearDepth.New()
	return nil
}

func (m *BindModule) Arguments() map[string]modulecapabilities.GraphQLArgument {
	arguments := map[string]modulecapabilities.GraphQLArgument{}
	for name, arg := range m.nearTextGraphqlProvider.Arguments() {
		arguments[name] = arg
	}
	for name, arg := range m.nearImageGraphqlProvider.Arguments() {
		arguments[name] = arg
	}
	for name, arg := range m.nearAudioGraphqlProvider.Arguments() {
		arguments[name] = arg
	}
	for name, arg := range m.nearVideoGraphqlProvider.Arguments() {
		arguments[name] = arg
	}
	for name, arg := range m.nearIMUGraphqlProvider.Arguments() {
		arguments[name] = arg
	}
	for name, arg := range m.nearThermalGraphqlProvider.Arguments() {
		arguments[name] = arg
	}
	for name, arg := range m.nearDepthGraphqlProvider.Arguments() {
		arguments[name] = arg
	}
	return arguments
}

func (m *BindModule) VectorSearches() map[string]modulecapabilities.VectorForParams[[]float32] {
	vectorSearches := map[string]modulecapabilities.VectorForParams[[]float32]{}
	for name, arg := range m.nearTextSearcher.VectorSearches() {
		vectorSearches[name] = arg
	}
	for name, arg := range m.nearImageSearcher.VectorSearches() {
		vectorSearches[name] = arg
	}
	for name, arg := range m.nearAudioSearcher.VectorSearches() {
		vectorSearches[name] = arg
	}
	for name, arg := range m.nearVideoSearcher.VectorSearches() {
		vectorSearches[name] = arg
	}
	for name, arg := range m.nearIMUSearcher.VectorSearches() {
		vectorSearches[name] = arg
	}
	for name, arg := range m.nearThermalSearcher.VectorSearches() {
		vectorSearches[name] = arg
	}
	for name, arg := range m.nearDepthSearcher.VectorSearches() {
		vectorSearches[name] = arg
	}
	return vectorSearches
}

var (
	_ = modulecapabilities.GraphQLArguments(New())
	_ = modulecapabilities.Searcher[[]float32](New())
)
