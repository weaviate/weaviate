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

package schema

import (
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/entities/versioned"
	configRuntime "github.com/weaviate/weaviate/usecases/config/runtime"
)

type ClassGetter struct {
	parser        *Parser
	schemaReader  SchemaReader
	schemaManager SchemaManager
	logger        logrus.FieldLogger

	collectionRetrievalStrategy *configRuntime.FeatureFlag[string]
}

func NewClassGetter(
	schemaParser *Parser,
	schemaManager SchemaManager,
	schemaReader SchemaReader,
	collectionRetrievalStrategyFF *configRuntime.FeatureFlag[string],
	logger logrus.FieldLogger,
) *ClassGetter {
	return &ClassGetter{
		parser:                      schemaParser,
		schemaReader:                schemaReader,
		schemaManager:               schemaManager,
		logger:                      logger,
		collectionRetrievalStrategy: collectionRetrievalStrategyFF,
	}
}

func (cg *ClassGetter) getClasses(names []string) (map[string]versioned.Class, error) {
	switch configRuntime.CollectionRetrievalStrategy(cg.collectionRetrievalStrategy.Get()) {
	case configRuntime.LeaderOnly:
		return cg.getClassesLeaderOnly(names)
	case configRuntime.LeaderOnMismatch:
		return cg.getClassesLeaderOnMismatch(names)
	case configRuntime.LocalOnly:
		return cg.getClassesLocalOnly(names)

		// This can happen if the feature flag gets configured with an invalid strategy
	default:
		return cg.getClassesLeaderOnly(names)
	}
}

func (cg *ClassGetter) getClassesLeaderOnly(names []string) (map[string]versioned.Class, error) {
	vclasses, err := cg.schemaManager.QueryReadOnlyClasses(names...)
	if err != nil {
		return nil, err
	}

	if len(vclasses) == 0 {
		return nil, nil
	}

	for _, vclass := range vclasses {
		if err := cg.parser.ParseClass(vclass.Class); err != nil {
			// remove invalid classes
			cg.logger.WithFields(logrus.Fields{
				"class": vclass.Class.Class,
				"error": err,
			}).Warn("parsing class error")
			delete(vclasses, vclass.Class.Class)
			continue
		}
	}

	return vclasses, nil
}

func (cg *ClassGetter) getClassesLocalOnly(names []string) (map[string]versioned.Class, error) {
	vclasses := map[string]versioned.Class{}
	for _, name := range names {
		vc := cg.schemaReader.ReadOnlyVersionedClass(name)
		if vc.Class == nil {
			cg.logger.WithFields(logrus.Fields{
				"class": name,
			}).Debug("could not find class in local schema")
			continue
		}
		vclasses[name] = vc
	}

	// Check if we have all the classes from the local schema
	if len(vclasses) < len(names) {
		missingClasses := []string{}
		for _, name := range names {
			if _, ok := vclasses[name]; !ok {
				missingClasses = append(missingClasses, name)
			}
		}
		cg.logger.WithFields(logrus.Fields{
			"missing":    missingClasses,
			"suggestion": "This node received a data request for a class that is not present on the local schema on the node. If the class was just updated in the schema and you want to be able to query it immediately consider changing the " + configRuntime.CollectionRetrievalStrategyEnvVariable + " config to \"" + configRuntime.LeaderOnly + "\".",
		}).Warn("not all classes found locally")
	}
	return vclasses, nil
}

func (cg *ClassGetter) getClassesLeaderOnMismatch(names []string) (map[string]versioned.Class, error) {
	classVersions, err := cg.schemaManager.QueryClassVersions(names...)
	if err != nil {
		return nil, err
	}
	versionedClassesToReturn := map[string]versioned.Class{}
	versionedClassesToQueryFromLeader := []string{}
	for _, name := range names {
		localVclass := cg.schemaReader.ReadOnlyVersionedClass(name)
		// First check if we have the class locally, if not make sure we'll query from leader
		if localVclass.Class == nil {
			versionedClassesToQueryFromLeader = append(versionedClassesToQueryFromLeader, name)
			continue
		}

		// We have the class locally, compare the version from leader (if any) and add to query to leader if we have to refresh the version
		leaderClassVersion, ok := classVersions[name]
		// < leaderClassVersion instead of != because there is some chance that the local version
		// could be ahead of the version returned by the leader if the response from the leader was
		// delayed and i don't think it would be helpful to query the leader again in that case as
		// it would likely return a version that is at least as large as the local version.
		if !ok || localVclass.Version < leaderClassVersion {
			versionedClassesToQueryFromLeader = append(versionedClassesToQueryFromLeader, name)
			continue
		}

		// We can use the local class version has not changed
		versionedClassesToReturn[name] = localVclass
	}
	if len(versionedClassesToQueryFromLeader) == 0 {
		return versionedClassesToReturn, nil
	}

	versionedClassesFromLeader, err := cg.schemaManager.QueryReadOnlyClasses(versionedClassesToQueryFromLeader...)
	if err != nil || len(versionedClassesFromLeader) == 0 {
		cg.logger.WithFields(logrus.Fields{
			"classes":    versionedClassesToQueryFromLeader,
			"error":      err,
			"suggestion": "This node received a data request for a class that is not present on the local schema on the node. If the class was just updated in the schema and you want to be able to query it immediately consider changing the " + configRuntime.CollectionRetrievalStrategyEnvVariable + " config to \"" + configRuntime.LeaderOnly + "\".",
		}).Warn("unable to query classes from leader")
		// return as many classes as we could get (to match previous behavior of the caller)
		return versionedClassesToReturn, err
	}

	// We only need to ParseClass the ones we receive from the leader due to the Class model containing `interface{}` that are broken on
	// marshall/unmarshall with gRPC.
	for _, vclass := range versionedClassesFromLeader {
		if err := cg.parser.ParseClass(vclass.Class); err != nil {
			// silently remove invalid classes to match previous behavior
			cg.logger.WithFields(logrus.Fields{
				"class": vclass.Class.Class,
				"error": err,
			}).Warn("parsing class error")
			delete(versionedClassesFromLeader, vclass.Class.Class)
			continue
		}
		versionedClassesToReturn[vclass.Class.Class] = vclass
	}

	return versionedClassesToReturn, nil
}
