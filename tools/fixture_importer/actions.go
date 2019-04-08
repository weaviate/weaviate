/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2019 Weaviate. All rights reserved.
 * LICENSE: https://github.com/creativesoftwarefdn/weaviate/blob/develop/LICENSE.md
 * DESIGN & CONCEPT: Bob van Luijt (@bobvanluijt)
 * CONTACT: hello@creativesoftwarefdn.org
 */
package main

import (
	"fmt"
	"strconv"
	"time"

	"github.com/creativesoftwarefdn/weaviate/client/actions"
	"github.com/creativesoftwarefdn/weaviate/models"
	spew "github.com/davecgh/go-spew/spew"
	"github.com/go-openapi/strfmt"
)

func createActions() {
	for _, action := range demoDataset.Actions {
		className := action["class"].(string)
		classKinds[className] = "Action"
		uuid := action["uuid"].(string)

		properties := map[string]interface{}{}

		for key, value := range action {
			if key == "class" || key == "uuid" {
				continue
			}

			ref, isRef := value.(map[string]interface{})
			if isRef {
				actionFixups = append(actionFixups, fixupAddRef{
					fromId:       uuid,
					fromProperty: key,
					toClass:      ref["class"].(string),
					toId:         ref["uuid"].(string),
				})
			} else {
				class := findClass(schema.Actions, className)
				property := findProperty(class, key)
				if len(property.AtDataType) != 1 {
					panic(fmt.Sprintf("Only one datatype supported for import. Failed in action %s.%s", className, property.Name))
				}
				dataType := property.AtDataType[0]

				switch dataType {
				case "string", "date", "text":
					properties[key] = value
				case "int":
					// Attempt to parse as float64 first.
					valFloat, ok := value.(float64)
					if ok {
						properties[key] = valFloat
					} else {
						v, err := strconv.ParseInt(value.(string), 10, 64)
						if err != nil {
							panic(err)
						}
						properties[key] = v
					}
				case "number":
					properties[key] = value.(float64)
				case "boolean":
					properties[key] = value.(bool)
				default:
					panic(fmt.Sprintf("No such datatype supported: %s", dataType))
				}
			}
		}

		t := models.ActionCreate{
			AtContext: "http://example.org",
			AtClass:   className,
			Schema:    properties,
		}

		action := assertCreateAction(&t)
		idMap[uuid] = string(action.ActionID) // Store mapping of ID's
		fmt.Printf("Created Action %s\n", action.ActionID)
	}
}

func fixupActions() {
	fmt.Printf("Checking if all actions that need a patch are created.\n")
	for {
		allExist := true

		for _, fixup := range actionFixups {
			if !checkActionExists(idMap[fixup.fromId]) {
				allExist = false
				fmt.Printf("From does not exist! %v\n", idMap[fixup.fromId])
			}
			if classKinds[fixup.toClass] == "Action" {
				if !checkActionExists(idMap[fixup.toId]) {
					allExist = false
					fmt.Printf("To action does not exist! %v\n", idMap[fixup.toId])
					break
				}
			} else { // assume it's a thing.
				if !checkThingExists(idMap[fixup.toId]) {
					allExist = false
					fmt.Printf("To thing does not exist! %v\n", idMap[fixup.toId])
					break
				}
			}
		}

		if allExist {
			fmt.Printf("Everything that needs to be patched exists!\n")
			break
		} else {
			fmt.Printf("Not everything that needs to be patched exists\n")

			var waitSecondsUntilSettled time.Duration = 2 * time.Second
			fmt.Printf("Waiting for %v to settle\n", waitSecondsUntilSettled)
			time.Sleep(waitSecondsUntilSettled)
			continue
		}
	}

	// Now fix up refs
	op := "add"
	for _, fixup := range actionFixups {
		path := fmt.Sprintf("/schema/%s", fixup.fromProperty)
		_, ok := classKinds[fixup.toClass]

		if !ok {
			panic(fmt.Sprintf("Unknown class '%s'", fixup.toClass))
		}

		patch := &models.PatchDocument{
			Op:   &op,
			Path: &path,
			Value: map[string]interface{}{
				"$cref": fmt.Sprintf("weaviate://localhost/things/%s", idMap[fixup.toId]),
			},
		}

		assertPatchAction(idMap[fixup.fromId], patch)
		fmt.Printf("Patched action %s\n", idMap[fixup.fromId])
	}
}

func checkActionExists(id string) bool {
	params := actions.NewWeaviateActionsGetParams().WithActionID(strfmt.UUID(id))
	resp, err := client.Actions.WeaviateActionsGet(params, nil)

	if err != nil {
		switch v := err.(type) {
		case *actions.WeaviateActionsGetNotFound:
			return false
		default:
			panic(fmt.Sprintf("Can't create action %#v, because %#v", resp, spew.Sdump(v)))
		}
	}

	return true
}

func assertCreateAction(t *models.ActionCreate) *models.ActionGetResponse {
	params := actions.NewWeaviateActionsCreateParams().WithBody(actions.WeaviateActionsCreateBody{Action: t})

	resp, err := client.Actions.WeaviateActionsCreate(params, nil)

	if err != nil {
		switch v := err.(type) {
		case *actions.WeaviateActionsCreateUnprocessableEntity:
			panic(fmt.Sprintf("Can't create action %#v, because %s", t, joinErrorMessages(v.Payload)))
		default:
			panic(fmt.Sprintf("Can't create action %#v, because %#v", t, spew.Sdump(err)))
		}
	}

	return resp.Payload
}

func assertPatchAction(id string, p *models.PatchDocument) *models.ActionGetResponse {
	params := actions.NewWeaviateActionsPatchParams().WithBody([]*models.PatchDocument{p}).WithActionID(strfmt.UUID(id))

	resp, err := client.Actions.WeaviateActionsPatch(params, nil)

	if err != nil {
		switch v := err.(type) {
		case *actions.WeaviateActionsPatchNotFound:
			panic(fmt.Sprintf("Can't patch action with %s, because action cannot be found", spew.Sdump(p)))
		case *actions.WeaviateActionsPatchUnprocessableEntity:
			panic(fmt.Sprintf("Can't patch action, because %s", joinErrorMessages(v.Payload)))
		default:
			_ = v
			panic(fmt.Sprintf("Can't patch action with %#v, because %#v", p, spew.Sdump(err)))
		}
	}

	return resp.Payload
}
