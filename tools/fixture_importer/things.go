/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2018 Weaviate. All rights reserved.
 * LICENSE: https://github.com/creativesoftwarefdn/weaviate/blob/develop/LICENSE.md
 * AUTHOR: Bob van Luijt (bob@kub.design)
 * See www.creativesoftwarefdn.org for details
 * Contact: @CreativeSofwFdn / bob@kub.design
 */
package main

import (
	"fmt"
	"github.com/creativesoftwarefdn/weaviate/client/things"
	"github.com/creativesoftwarefdn/weaviate/models"
	spew "github.com/davecgh/go-spew/spew"
	"github.com/go-openapi/strfmt"
	"strconv"
	"time"
)

func createThings() {
	for _, thing := range demoDataset.Things {
		className := thing["class"].(string)
		classKinds[className] = "Thing"
		uuid := thing["uuid"].(string)

		properties := map[string]interface{}{}

		for key, value := range thing {
			if key == "class" || key == "uuid" {
				continue
			}

			ref, isRef := value.(map[string]interface{})
			if isRef {
				thingFixups = append(thingFixups, fixupAddRef{
					fromId:       uuid,
					fromProperty: key,
					toClass:      ref["class"].(string),
					toId:         ref["uuid"].(string),
				})
			} else {
				class := findClass(schema.Things, className)
				property := findProperty(class, key)
				if len(property.AtDataType) != 1 {
					panic(fmt.Sprintf("Only one datatype supported for import. Failed in thing %s.%s", className, property.Name))
				}
				dataType := property.AtDataType[0]

				switch dataType {
				case "string", "date":
					properties[key] = value
				case "int":
					switch typedValue := value.(type) {
					case string:
						v, err := strconv.ParseInt(typedValue, 10, 64)
						if err != nil {
							panic(err)
						}
						properties[key] = v
					case float64:
						properties[key] = int(typedValue)
					default:
						panic("Unexpected type")
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

		t := models.ThingCreate{
			AtContext: "http://example.org",
			AtClass:   className,
			Schema:    properties,
		}

		thing := assertCreateThing(&t)
		idMap[uuid] = string(thing.ThingID) // Store mapping of ID's
		fmt.Printf("Created Thing %s\n", thing.ThingID)
	}
}

func fixupThings() {
	fmt.Printf("Checking if all things that need a patch are created.\n")
	for {
		allExist := true

		for _, fixup := range thingFixups {
			if !checkThingExists(idMap[fixup.fromId]) {
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
	for _, fixup := range thingFixups {
		path := fmt.Sprintf("/schema/%s", fixup.fromProperty)
		kind, ok := classKinds[fixup.toClass]

		if !ok {
			panic(fmt.Sprintf("Unknown class '%s'", fixup.toClass))
		}

		patch := &models.PatchDocument{
			Op:   &op,
			Path: &path,
			Value: map[string]interface{}{
				"$cref":       idMap[fixup.toId],
				"locationUrl": "http://localhost:8080",
				"type":        kind,
			},
		}

		assertPatchThing(idMap[fixup.fromId], patch)
		fmt.Printf("Patched thing %s\n", idMap[fixup.fromId])
	}
}

func checkThingExists(id string) bool {
	params := things.NewWeaviateThingsGetParams().WithThingID(strfmt.UUID(id))
	resp, err := client.Things.WeaviateThingsGet(params, auth)

	if err != nil {
		switch v := err.(type) {
		case *things.WeaviateThingsGetNotFound:
			return false
		default:
			panic(fmt.Sprintf("Can't create thing %#v, because %#v", resp, spew.Sdump(v)))
		}
	}

	return true
}

func assertCreateThing(t *models.ThingCreate) *models.ThingGetResponse {
	params := things.NewWeaviateThingsCreateParams().WithBody(things.WeaviateThingsCreateBody{Thing: t})

	resp, _, err := client.Things.WeaviateThingsCreate(params, auth)

	if err != nil {
		switch v := err.(type) {
		case *things.WeaviateThingsCreateUnprocessableEntity:
			panic(fmt.Sprintf("Can't create thing %#v, because %s", t, joinErrorMessages(v.Payload)))
		default:
			panic(fmt.Sprintf("Can't create thing %#v, because %#v", t, spew.Sdump(err)))
		}
	}

	return resp.Payload
}

func assertUpdateThing(id string, update *models.ThingUpdate) *models.ThingGetResponse {
	params := things.NewWeaviateThingsUpdateParams().WithBody(update).WithThingID(strfmt.UUID(id))

	resp, err := client.Things.WeaviateThingsUpdate(params, auth)

	if err != nil {
		switch v := err.(type) {
		case *things.WeaviateThingsUpdateNotFound:
			panic(fmt.Sprintf("Can't patch thing with %s, because thing cannot be found", spew.Sdump(update)))
		case *things.WeaviateThingsUpdateUnprocessableEntity:
			panic(fmt.Sprintf("Can't patch thing, because %s (patch: %#v)", joinErrorMessages(v.Payload), *update))
		default:
			_ = v
			panic(fmt.Sprintf("Can't patch thing with %#v, because %#v", update, spew.Sdump(err)))
		}
	}

	return resp.Payload
}

func assertPatchThing(id string, p *models.PatchDocument) *models.ThingGetResponse {
	params := things.NewWeaviateThingsPatchParams().WithBody([]*models.PatchDocument{p}).WithThingID(strfmt.UUID(id))

	resp, _, err := client.Things.WeaviateThingsPatch(params, auth)

	if err != nil {
		switch v := err.(type) {
		case *things.WeaviateThingsPatchNotFound:
			panic(fmt.Sprintf("Can't patch thing with %s, because thing cannot be found", spew.Sdump(p)))
		case *things.WeaviateThingsPatchUnprocessableEntity:
			panic(fmt.Sprintf("Can't patch thing, because %s", joinErrorMessages(v.Payload)))
		default:
			_ = v
			panic(fmt.Sprintf("Can't patch thing with %#v, because %#v", p, spew.Sdump(err)))
		}
	}

	return resp.Payload
}
