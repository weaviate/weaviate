//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2019 SeMI Holding B.V. (registered @ Dutch Chamber of Commerce no 75221632). All rights reserved.
//  LICENSE WEAVIATE OPEN SOURCE: https://www.semi.technology/playbook/playbook/contract-weaviate-OSS.html
//  LICENSE WEAVIATE ENTERPRISE: https://www.semi.technology/playbook/contract-weaviate-enterprise.html
//  CONCEPT: Bob van Luijt (@bobvanluijt)
//  CONTACT: hello@semi.technology
//

package schema

import (
	"context"

	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/entities/schema/kind"
)

// UpdateAction which exists
func (m *Manager) UpdateAction(ctx context.Context, principal *models.Principal,
	name string, class *models.Class) error {

	err := m.authorizer.Authorize(principal, "update", "schema/actions")
	if err != nil {
		return err
	}

	return m.updateClass(ctx, name, class, kind.Action)
}

// UpdateThing which exists
func (m *Manager) UpdateThing(ctx context.Context, principal *models.Principal,
	name string, class *models.Class) error {

	err := m.authorizer.Authorize(principal, "update", "schema/things")
	if err != nil {
		return err
	}

	return m.updateClass(ctx, name, class, kind.Thing)
}

// TODO: gh-832: Implement full capabilities, not just keywords/naming
func (m *Manager) updateClass(ctx context.Context, className string,
	class *models.Class, k kind.Kind) error {
	unlock, err := m.locks.LockSchema()
	if err != nil {
		return err
	}
	defer unlock()

	var newName *string
	var newKeywords *models.Keywords

	if class.Class != className {
		// the name in the URI and body don't match, so we assume the user wants to rename
		n := upperCaseClassName(class.Class)
		newName = &n
	}

	// TODO gh-619: This implies that we can't undo setting keywords, because we can't detect if keywords is not present, or empty.
	if len(class.Keywords) > 0 {
		newKeywords = &class.Keywords
	}

	semanticSchema := m.state.SchemaFor(k)

	class, err = schema.GetClassByName(semanticSchema, className)
	if err != nil {
		return err
	}

	classNameAfterUpdate := className
	keywordsAfterUpdate := class.Keywords

	// First validate the request
	if newName != nil {
		err = m.validateClassNameUniqueness(*newName)
		classNameAfterUpdate = *newName
		if err != nil {
			return err
		}
	}

	if newKeywords != nil {
		keywordsAfterUpdate = *newKeywords
	}

	// Validate name / keywords in contextionary
	if err = m.validateClassNameAndKeywords(ctx, k, classNameAfterUpdate, keywordsAfterUpdate); err != nil {
		return err
	}

	// Validated! Now apply the changes.
	class.Class = classNameAfterUpdate
	class.Keywords = keywordsAfterUpdate

	err = m.saveSchema(ctx)

	if err != nil {
		return nil
	}

	return m.migrator.UpdateClass(ctx, k, className, newName, newKeywords)
}
