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
	"fmt"

	"github.com/davecgh/go-spew/spew"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/entities/versioned"
	"github.com/weaviate/weaviate/usecases/config"
)

type classGetter interface {
	getClasses(names []string) (map[string]versioned.Class, error)
}

func NewClassGetter(schemaRetrievalStrategy config.SchemaRetrievalStrategy, schemaParser *Parser, schemaManager SchemaManager, schemaReader SchemaReader, logger logrus.FieldLogger) (classGetter, error) {
	switch schemaRetrievalStrategy {
	case config.LeaderOnly:
		return newClassGetterLeaderOnly(schemaParser, schemaManager, logger), nil
	case config.LocalOnly:
		return newClassGetterLocalOnly(schemaParser, schemaReader, logger), nil
	case config.LeaderOnMismatch:
		return newClassGetterLeaderOnMismatch(schemaParser, schemaReader, schemaManager, logger), nil
	default:
		return nil, fmt.Errorf("unknown class getter method: %s", config.SchemaRetrievalStrategyToString[schemaRetrievalStrategy])
	}
}

type classGetterLeaderOnly struct {
	parser        *Parser
	schemaManager SchemaManager
	logger        logrus.FieldLogger
}

func newClassGetterLeaderOnly(parser *Parser, schemaManager SchemaManager, logger logrus.FieldLogger) *classGetterLeaderOnly {
	return &classGetterLeaderOnly{
		parser:        parser,
		schemaManager: schemaManager,
		logger:        logger,
	}
}

type classGetterLocalOnly struct {
	parser       *Parser
	schemaReader SchemaReader
	logger       logrus.FieldLogger
}

func newClassGetterLocalOnly(parser *Parser, schemaReader SchemaReader, logger logrus.FieldLogger) *classGetterLocalOnly {
	return &classGetterLocalOnly{
		parser:       parser,
		schemaReader: schemaReader,
		logger:       logger,
	}
}

type classGetterLeaderOnMismatch struct {
	parser        *Parser
	schemaReader  SchemaReader
	schemaManager SchemaManager
	logger        logrus.FieldLogger
}

func newClassGetterLeaderOnMismatch(parser *Parser, schemaReader SchemaReader, schemaManager SchemaManager, logger logrus.FieldLogger) *classGetterLeaderOnMismatch {
	return &classGetterLeaderOnMismatch{
		parser:        parser,
		schemaReader:  schemaReader,
		schemaManager: schemaManager,
		logger:        logger,
	}
}

func (cg *classGetterLeaderOnly) getClasses(names []string) (map[string]versioned.Class, error) {
	vclasses, err := cg.schemaManager.QueryReadOnlyClasses(names...)
	if err != nil {
		return nil, err
	}

	if len(vclasses) == 0 {
		return nil, nil
	}

	for _, vclass := range vclasses {
		if err := cg.parser.ParseClass(vclass.Class); err != nil {
			spew.Dump(err)
			// remove invalid classes
			cg.logger.WithFields(logrus.Fields{
				"Class": vclass.Class.Class,
				"Error": err,
			}).Warn("parsing class error")
			delete(vclasses, vclass.Class.Class)
			continue
		}
	}

	return vclasses, nil
}

func (cg *classGetterLocalOnly) getClasses(names []string) (map[string]versioned.Class, error) {
	vclasses := map[string]versioned.Class{}
	for _, name := range names {
		vc, err := cg.schemaReader.ReadOnlyVersionedClass(name)
		if err != nil {
			cg.logger.WithFields(logrus.Fields{
				"Class": vc.Class.Class,
				"Error": err,
			}).Warn("error reading local class")
			continue
		}
		vclasses[name] = vc
	}

	for _, vclass := range vclasses {
		if err := cg.parser.ParseClass(vclass.Class); err != nil {
			spew.Dump(err)
			// remove invalid classes
			cg.logger.WithFields(logrus.Fields{
				"Class": vclass.Class.Class,
				"Error": err,
			}).Warn("parsing class error")
			delete(vclasses, vclass.Class.Class)
			continue
		}
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
			"suggestion": "This node received a data request for a class that is not present on the local schema on the node. If the class was just updated in the schema and you want to be able to query it immediately consider changing the " + config.SchemaRetrievalStrategyEnvVariable + " config to \"" + config.SchemaRetrievalStrategyToString[config.LeaderOnly] + "\".",
		}).Warn("not all classes found locally")
	}
	return vclasses, nil
}

func (cg *classGetterLeaderOnMismatch) getClasses(names []string) (map[string]versioned.Class, error) {
	classVersions, err := cg.schemaManager.QueryClassVersions(names...)
	if err != nil {
		return nil, err
	}
	versionedClassesToReturn := map[string]versioned.Class{}
	versionedClassesToQueryFromLeader := []string{}
	for _, name := range names {
		localVclass, err := cg.schemaReader.ReadOnlyVersionedClass(name)
		leaderClassVersion, ok := classVersions[name]
		// < leaderClassVersion instead of != because there is some chance that the local version
		// could be ahead of the version returned by the leader if the response from the leader was
		// delayed and i don't think it would be helpful to query the leader again in that case as
		// it would likely return a version that is at least as large as the local version.
		if err != nil || !ok || localVclass.Version < leaderClassVersion {
			versionedClassesToQueryFromLeader = append(versionedClassesToQueryFromLeader, name)
			continue
		}
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
			"suggestion": "This node received a data request for a class that is not present on the local schema on the node. If the class was just updated in the schema and you want to be able to query it immediately consider changing the " + config.SchemaRetrievalStrategyEnvVariable + " config to \"" + config.SchemaRetrievalStrategyToString[config.LeaderOnly] + "\".",
		}).Warn("unable to query classes from leader")
		// return as many classes as we could get (to match previous behavior of the caller)
		return versionedClassesToReturn, err
	}

	for _, vclass := range versionedClassesFromLeader {
		if err := cg.parser.ParseClass(vclass.Class); err != nil {
			// silently remove invalid classes to match previous behavior
			cg.logger.WithFields(logrus.Fields{
				"Class": vclass.Class.Class,
				"Error": err,
			}).Warn("parsing class error")
			delete(versionedClassesFromLeader, vclass.Class.Class)
			continue
		}
		versionedClassesToReturn[vclass.Class.Class] = vclass
	}

	return versionedClassesToReturn, nil
}
