/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 Weaviate. All rights reserved.
 * LICENSE: https://github.com/weaviate/weaviate/blob/master/LICENSE
 * AUTHOR: Bob van Luijt (bob@weaviate.com)
 * See www.weaviate.com for details
 * Contact: @weaviate_iot / yourfriends@weaviate.com
 */
 package restapi




import (
	"encoding/json"
)

// SwaggerJSON embedded version of the swagger document used at generation time
var SwaggerJSON json.RawMessage

func init() {
	SwaggerJSON = json.RawMessage([]byte(`{
  "consumes": [
    "application/json",
    "application/xml",
    "application/x-yaml",
    "text/plain",
    "application/octet-stream",
    "multipart/form-data",
    "application/x-www-form-urlencoded",
    "application/json-patch+json"
  ],
  "produces": [
    "application/json",
    "application/xml",
    "application/x-yaml",
    "text/plain",
    "application/octet-stream",
    "multipart/form-data",
    "application/x-www-form-urlencoded"
  ],
  "schemes": [
    "https"
  ],
  "swagger": "2.0",
  "info": {
    "description": "Ubiquitous Computing (\u0026 Internet of Things) platform that lets you manage cloud ready things directly or by proxy. More info: https://github.com/weaviate/weaviate",
    "title": "Weaviate API and MQTT Ubiquitous Computing platform.",
    "contact": {
      "name": "Weaviate",
      "url": "https://github.com/weaviate/weaviate",
      "email": "bob@weaviate.com"
    },
    "version": "v0.2.5"
  },
  "basePath": "/weaviate/v1",
  "paths": {
    "/commands": {
      "get": {
        "description": "Lists all commands in reverse order of creation.",
        "tags": [
          "commands"
        ],
        "summary": "Get a list of commands related to this key.",
        "operationId": "weaviate.commands.list",
        "parameters": [
          {
            "$ref": "#/parameters/CommonMaxResultsParameterQuery"
          },
          {
            "$ref": "#/parameters/CommonPageParameterQuery"
          }
        ],
        "responses": {
          "200": {
            "description": "Successful response.",
            "schema": {
              "$ref": "#/definitions/CommandsListResponse"
            }
          },
          "401": {
            "description": "Unauthorized or invalid credentials."
          },
          "403": {
            "description": "The used API-key has insufficient permissions."
          },
          "404": {
            "description": "Successful query result but no resource was found."
          },
          "501": {
            "description": "Not (yet) implemented."
          }
        },
        "x-available-in-mqtt": false
      },
      "post": {
        "description": "Creates and sends a new command.",
        "tags": [
          "commands"
        ],
        "summary": "Create a new command related to this key related to this key.",
        "operationId": "weaviate.commands.create",
        "parameters": [
          {
            "name": "body",
            "in": "body",
            "required": true,
            "schema": {
              "$ref": "#/definitions/CommandCreate"
            }
          }
        ],
        "responses": {
          "202": {
            "description": "Successfully received.",
            "schema": {
              "$ref": "#/definitions/CommandGetResponse"
            }
          },
          "401": {
            "description": "Unauthorized or invalid credentials."
          },
          "403": {
            "description": "The used API-key has insufficient permissions."
          },
          "422": {
            "description": "Can not validate, check the body."
          },
          "501": {
            "description": "Not (yet) implemented."
          }
        },
        "x-available-in-mqtt": false
      }
    },
    "/commands/{commandId}": {
      "get": {
        "description": "Returns a particular command.",
        "tags": [
          "commands"
        ],
        "summary": "Get a command based on its uuid related to this key.",
        "operationId": "weaviate.commands.get",
        "parameters": [
          {
            "type": "string",
            "description": "Unique command ID.",
            "name": "commandId",
            "in": "path",
            "required": true
          }
        ],
        "responses": {
          "200": {
            "description": "Successful response.",
            "schema": {
              "$ref": "#/definitions/CommandGetResponse"
            }
          },
          "401": {
            "description": "Unauthorized or invalid credentials."
          },
          "403": {
            "description": "The used API-key has insufficient permissions."
          },
          "404": {
            "description": "Successful query result but no resource was found."
          },
          "501": {
            "description": "Not (yet) implemented."
          }
        },
        "x-available-in-mqtt": false
      },
      "put": {
        "description": "Updates a command. This method may be used only by things.",
        "tags": [
          "commands"
        ],
        "summary": "Update a command based on its uuid related to this key.",
        "operationId": "weaviate.commands.update",
        "parameters": [
          {
            "type": "string",
            "description": "Unique command ID.",
            "name": "commandId",
            "in": "path",
            "required": true
          },
          {
            "name": "body",
            "in": "body",
            "required": true,
            "schema": {
              "$ref": "#/definitions/CommandUpdate"
            }
          }
        ],
        "responses": {
          "200": {
            "description": "Successful updated.",
            "schema": {
              "$ref": "#/definitions/CommandGetResponse"
            }
          },
          "401": {
            "description": "Unauthorized or invalid credentials."
          },
          "403": {
            "description": "The used API-key has insufficient permissions."
          },
          "404": {
            "description": "Successful query result but no resource was found."
          },
          "422": {
            "description": "Can not validate, check the body."
          },
          "501": {
            "description": "Not (yet) implemented."
          }
        },
        "x-available-in-mqtt": false
      },
      "delete": {
        "description": "Deletes a command.",
        "tags": [
          "commands"
        ],
        "summary": "Delete a command based on its uuid related to this key.",
        "operationId": "weaviate.commands.delete",
        "parameters": [
          {
            "type": "string",
            "format": "uuid",
            "description": "Unique command ID.",
            "name": "commandId",
            "in": "path",
            "required": true
          }
        ],
        "responses": {
          "204": {
            "description": "Successful deleted."
          },
          "401": {
            "description": "Unauthorized or invalid credentials."
          },
          "403": {
            "description": "The used API-key has insufficient permissions."
          },
          "404": {
            "description": "Successful query result but no resource was found."
          },
          "501": {
            "description": "Not (yet) implemented."
          }
        },
        "x-available-in-mqtt": false
      },
      "patch": {
        "description": "Updates a command. This method may be used only by things. This method supports patch semantics.",
        "tags": [
          "commands"
        ],
        "summary": "Update a command based on its uuid by using patch semantics related to this key.",
        "operationId": "weaviate.commands.patch",
        "parameters": [
          {
            "type": "string",
            "description": "Unique command ID.",
            "name": "commandId",
            "in": "path",
            "required": true
          },
          {
            "description": "JSONPatch document as defined by RFC 6902.",
            "name": "body",
            "in": "body",
            "required": true,
            "schema": {
              "type": "array",
              "items": {
                "$ref": "#/definitions/PatchDocument"
              }
            }
          }
        ],
        "responses": {
          "200": {
            "description": "Successful updated.",
            "schema": {
              "$ref": "#/definitions/CommandGetResponse"
            }
          },
          "400": {
            "description": "The patch-JSON is malformed."
          },
          "401": {
            "description": "Unauthorized or invalid credentials."
          },
          "403": {
            "description": "The used API-key has insufficient permissions."
          },
          "404": {
            "description": "Successful query result but no resource was found."
          },
          "422": {
            "description": "The patch-JSON is valid but unprocessable."
          },
          "501": {
            "description": "Not (yet) implemented."
          }
        },
        "x-available-in-mqtt": false
      }
    },
    "/events/validate": {
      "post": {
        "description": "Validate a event object. It has to be based on a command, which is related to the given Thing(Template) to be accepted by this validation.",
        "tags": [
          "events"
        ],
        "summary": "Validate a event based on a command.",
        "operationId": "weaviate.events.validate",
        "parameters": [
          {
            "name": "body",
            "in": "body",
            "required": true,
            "schema": {
              "$ref": "#/definitions/EventValidate"
            }
          }
        ],
        "responses": {
          "200": {
            "description": "Successful validated."
          },
          "401": {
            "description": "Unauthorized or invalid credentials."
          },
          "403": {
            "description": "The used API-key has insufficient permissions."
          },
          "422": {
            "description": "Can not validate, check the body."
          },
          "501": {
            "description": "Not (yet) implemented."
          }
        },
        "x-available-in-mqtt": false
      }
    },
    "/events/{eventId}": {
      "get": {
        "description": "Lists events.",
        "tags": [
          "events"
        ],
        "summary": "Get a specific event based on its uuid and a thing uuid related to this key.",
        "operationId": "weaviate.events.get",
        "parameters": [
          {
            "type": "string",
            "format": "uuid",
            "description": "Unique ID of the event.",
            "name": "eventId",
            "in": "path",
            "required": true
          }
        ],
        "responses": {
          "200": {
            "description": "Successful response.",
            "schema": {
              "$ref": "#/definitions/EventGetResponse"
            }
          },
          "401": {
            "description": "Unauthorized or invalid credentials."
          },
          "403": {
            "description": "The used API-key has insufficient permissions."
          },
          "404": {
            "description": "Successful query result but no resource was found."
          },
          "501": {
            "description": "Not (yet) implemented."
          }
        },
        "x-available-in-mqtt": false
      }
    },
    "/groups": {
      "get": {
        "description": "Lists all groups.",
        "tags": [
          "groups"
        ],
        "summary": "Get a list of groups related to this key.",
        "operationId": "weaviate.groups.list",
        "parameters": [
          {
            "$ref": "#/parameters/CommonMaxResultsParameterQuery"
          },
          {
            "$ref": "#/parameters/CommonPageParameterQuery"
          }
        ],
        "responses": {
          "200": {
            "description": "Successful response.",
            "schema": {
              "$ref": "#/definitions/GroupsListResponse"
            }
          },
          "401": {
            "description": "Unauthorized or invalid credentials."
          },
          "403": {
            "description": "The used API-key has insufficient permissions."
          },
          "404": {
            "description": "Successful query result but no resource was found."
          },
          "501": {
            "description": "Not (yet) implemented."
          }
        },
        "x-available-in-mqtt": false
      },
      "post": {
        "description": "Creates group.",
        "tags": [
          "groups"
        ],
        "summary": "Create a new group related to this key.",
        "operationId": "weaviate.groups.create",
        "parameters": [
          {
            "name": "body",
            "in": "body",
            "required": true,
            "schema": {
              "$ref": "#/definitions/GroupCreate"
            }
          }
        ],
        "responses": {
          "202": {
            "description": "Successfully received.",
            "schema": {
              "$ref": "#/definitions/GroupGetResponse"
            }
          },
          "401": {
            "description": "Unauthorized or invalid credentials."
          },
          "403": {
            "description": "The used API-key has insufficient permissions."
          },
          "422": {
            "description": "Can not validate, check the body."
          },
          "501": {
            "description": "Not (yet) implemented."
          }
        },
        "x-available-in-mqtt": false
      }
    },
    "/groups/{groupId}": {
      "get": {
        "description": "Get a group.",
        "tags": [
          "groups"
        ],
        "summary": "Get a group based on its uuid related to this key.",
        "operationId": "weaviate.groups.get",
        "parameters": [
          {
            "type": "string",
            "description": "Unique ID of the group.",
            "name": "groupId",
            "in": "path",
            "required": true
          }
        ],
        "responses": {
          "200": {
            "description": "Successful response.",
            "schema": {
              "$ref": "#/definitions/GroupGetResponse"
            }
          },
          "401": {
            "description": "Unauthorized or invalid credentials."
          },
          "403": {
            "description": "The used API-key has insufficient permissions."
          },
          "404": {
            "description": "Successful query result but no resource was found."
          },
          "501": {
            "description": "Not (yet) implemented."
          }
        },
        "x-available-in-mqtt": false
      },
      "put": {
        "description": "Updates an group.",
        "tags": [
          "groups"
        ],
        "summary": "Update a group based on its uuid related to this key.",
        "operationId": "weaviate.groups.update",
        "parameters": [
          {
            "name": "body",
            "in": "body",
            "required": true,
            "schema": {
              "$ref": "#/definitions/GroupUpdate"
            }
          },
          {
            "type": "string",
            "description": "Unique ID of the group.",
            "name": "groupId",
            "in": "path",
            "required": true
          }
        ],
        "responses": {
          "200": {
            "description": "Successful updated.",
            "schema": {
              "$ref": "#/definitions/GroupGetResponse"
            }
          },
          "401": {
            "description": "Unauthorized or invalid credentials."
          },
          "403": {
            "description": "The used API-key has insufficient permissions."
          },
          "404": {
            "description": "Successful query result but no resource was found."
          },
          "422": {
            "description": "Can not validate, check the body."
          },
          "501": {
            "description": "Not (yet) implemented."
          }
        },
        "x-available-in-mqtt": false
      },
      "delete": {
        "description": "Deletes an group.",
        "tags": [
          "groups"
        ],
        "summary": "Delete a group based on its uuid related to this key.",
        "operationId": "weaviate.groups.delete",
        "parameters": [
          {
            "type": "string",
            "format": "uuid",
            "name": "groupId",
            "in": "path",
            "required": true
          }
        ],
        "responses": {
          "204": {
            "description": "Successful deleted."
          },
          "401": {
            "description": "Unauthorized or invalid credentials."
          },
          "403": {
            "description": "The used API-key has insufficient permissions."
          },
          "404": {
            "description": "Successful query result but no resource was found."
          },
          "501": {
            "description": "Not (yet) implemented."
          }
        },
        "x-available-in-mqtt": false
      },
      "patch": {
        "description": "Updates an group. This method supports patch semantics.",
        "tags": [
          "groups"
        ],
        "summary": "Update a group based on its uuid (using patch semantics) related to this key.",
        "operationId": "weaviate.groups.patch",
        "parameters": [
          {
            "type": "string",
            "description": "Unique ID of the group.",
            "name": "groupId",
            "in": "path",
            "required": true
          },
          {
            "description": "JSONPatch document as defined by RFC 6902.",
            "name": "body",
            "in": "body",
            "required": true,
            "schema": {
              "type": "array",
              "items": {
                "$ref": "#/definitions/PatchDocument"
              }
            }
          }
        ],
        "responses": {
          "200": {
            "description": "Successful updated.",
            "schema": {
              "$ref": "#/definitions/GroupGetResponse"
            }
          },
          "400": {
            "description": "The patch-JSON is malformed."
          },
          "401": {
            "description": "Unauthorized or invalid credentials."
          },
          "403": {
            "description": "The used API-key has insufficient permissions."
          },
          "404": {
            "description": "Successful query result but no resource was found."
          },
          "422": {
            "description": "The patch-JSON is valid but unprocessable."
          },
          "501": {
            "description": "Not (yet) implemented."
          }
        },
        "x-available-in-mqtt": false
      }
    },
    "/groups/{groupId}/events": {
      "get": {
        "description": "Lists events.",
        "tags": [
          "events"
        ],
        "summary": "Get a list of events based on a groups's uuid (also available as MQTT channel) related to this key.",
        "operationId": "weaviate.groups.events.list",
        "parameters": [
          {
            "type": "string",
            "format": "uuid",
            "description": "Unique ID of the group.",
            "name": "groupId",
            "in": "path",
            "required": true
          },
          {
            "$ref": "#/parameters/CommonMaxResultsParameterQuery"
          },
          {
            "$ref": "#/parameters/CommonPageParameterQuery"
          }
        ],
        "responses": {
          "200": {
            "description": "Successful response.",
            "schema": {
              "$ref": "#/definitions/EventsListResponse"
            }
          },
          "401": {
            "description": "Unauthorized or invalid credentials."
          },
          "403": {
            "description": "The used API-key has insufficient permissions."
          },
          "404": {
            "description": "Successful query result but no resource was found."
          },
          "501": {
            "description": "Not (yet) implemented."
          }
        },
        "x-available-in-mqtt": true
      },
      "post": {
        "description": "Create event in group.",
        "tags": [
          "events"
        ],
        "summary": "Create events for a group (also available as MQTT channel) related to this key.",
        "operationId": "weaviate.groups.events.create",
        "parameters": [
          {
            "type": "string",
            "description": "Unique ID of the group.",
            "name": "groupId",
            "in": "path",
            "required": true
          },
          {
            "name": "body",
            "in": "body",
            "required": true,
            "schema": {
              "$ref": "#/definitions/EventCreate"
            }
          }
        ],
        "responses": {
          "202": {
            "description": "Successfully received.",
            "schema": {
              "$ref": "#/definitions/EventGetResponse"
            }
          },
          "401": {
            "description": "Unauthorized or invalid credentials."
          },
          "403": {
            "description": "The used API-key has insufficient permissions."
          },
          "422": {
            "description": "Can not execute this command, because the commandParameters{} are set incorrectly."
          },
          "501": {
            "description": "Not (yet) implemented."
          }
        },
        "x-available-in-mqtt": false
      }
    },
    "/keys": {
      "post": {
        "description": "Creates a new key.",
        "tags": [
          "keys"
        ],
        "summary": "Create a new key related to this key.",
        "operationId": "weaviate.key.create",
        "parameters": [
          {
            "name": "body",
            "in": "body",
            "required": true,
            "schema": {
              "$ref": "#/definitions/KeyCreate"
            }
          }
        ],
        "responses": {
          "202": {
            "description": "Successfully received.",
            "schema": {
              "$ref": "#/definitions/KeyGetResponse"
            }
          },
          "401": {
            "description": "Unauthorized or invalid credentials."
          },
          "403": {
            "description": "The used API-key has insufficient permissions."
          },
          "422": {
            "description": "Can not validate, check the body."
          },
          "501": {
            "description": "Not (yet) implemented."
          }
        },
        "x-available-in-mqtt": false
      }
    },
    "/keys/{keyId}": {
      "get": {
        "description": "Get a key.",
        "tags": [
          "keys"
        ],
        "summary": "Get a key based on its uuid related to this key.",
        "operationId": "weaviate.keys.get",
        "parameters": [
          {
            "type": "string",
            "description": "Unique ID of the key.",
            "name": "keyId",
            "in": "path",
            "required": true
          }
        ],
        "responses": {
          "200": {
            "description": "Successful response.",
            "schema": {
              "$ref": "#/definitions/KeyGetResponse"
            }
          },
          "401": {
            "description": "Unauthorized or invalid credentials."
          },
          "403": {
            "description": "The used API-key has insufficient permissions."
          },
          "404": {
            "description": "Successful query result but no resource was found."
          },
          "501": {
            "description": "Not (yet) implemented."
          }
        },
        "x-available-in-mqtt": false
      },
      "delete": {
        "description": "Deletes a key. Only parent or self is allowed to delete key.",
        "tags": [
          "keys"
        ],
        "summary": "Delete a key based on its uuid related to this key.",
        "operationId": "weaviate.keys.delete",
        "parameters": [
          {
            "type": "string",
            "format": "uuid",
            "name": "keyId",
            "in": "path",
            "required": true
          }
        ],
        "responses": {
          "204": {
            "description": "Successful deleted."
          },
          "401": {
            "description": "Unauthorized or invalid credentials."
          },
          "403": {
            "description": "The used API-key has insufficient permissions."
          },
          "404": {
            "description": "Successful query result but no resource was found."
          },
          "501": {
            "description": "Not (yet) implemented."
          }
        },
        "x-available-in-mqtt": false
      }
    },
    "/keys/{keyId}/children": {
      "get": {
        "description": "Get children or a key, only one step deep. A child can have children of its own.",
        "tags": [
          "keys"
        ],
        "summary": "Get an object of this keys' children related to this key.",
        "operationId": "weaviate.keys.children.get",
        "parameters": [
          {
            "type": "string",
            "format": "uuid",
            "description": "Unique ID of the key.",
            "name": "keyId",
            "in": "path",
            "required": true
          }
        ],
        "responses": {
          "200": {
            "description": "Successful response.",
            "schema": {
              "$ref": "#/definitions/KeyChildren"
            }
          },
          "401": {
            "description": "Unauthorized or invalid credentials."
          },
          "403": {
            "description": "The used API-key has insufficient permissions."
          },
          "404": {
            "description": "Successful query result but no resource was found."
          },
          "501": {
            "description": "Not (yet) implemented"
          }
        },
        "x-available-in-mqtt": false
      }
    },
    "/locations": {
      "get": {
        "description": "Lists all locations.",
        "tags": [
          "locations"
        ],
        "summary": "Get a list of locations related to this key.",
        "operationId": "weaviate.locations.list",
        "parameters": [
          {
            "$ref": "#/parameters/CommonMaxResultsParameterQuery"
          },
          {
            "$ref": "#/parameters/CommonPageParameterQuery"
          }
        ],
        "responses": {
          "200": {
            "description": "Successful response.",
            "schema": {
              "$ref": "#/definitions/LocationsListResponse"
            }
          },
          "401": {
            "description": "Unauthorized or invalid credentials."
          },
          "403": {
            "description": "The used API-key has insufficient permissions."
          },
          "404": {
            "description": "Successful query result but no resource was found."
          },
          "501": {
            "description": "Not (yet) implemented."
          }
        },
        "x-available-in-mqtt": false
      },
      "post": {
        "description": "Create location.",
        "tags": [
          "locations"
        ],
        "summary": "Create a new location related to this key.",
        "operationId": "weaviate.locations.create",
        "parameters": [
          {
            "name": "body",
            "in": "body",
            "required": true,
            "schema": {
              "$ref": "#/definitions/LocationCreate"
            }
          }
        ],
        "responses": {
          "202": {
            "description": "Successfully received.",
            "schema": {
              "$ref": "#/definitions/LocationGetResponse"
            }
          },
          "401": {
            "description": "Unauthorized or invalid credentials."
          },
          "403": {
            "description": "The used API-key has insufficient permissions."
          },
          "422": {
            "description": "Can not validate, check the body."
          },
          "501": {
            "description": "Not (yet) implemented."
          }
        },
        "x-available-in-mqtt": false
      }
    },
    "/locations/{locationId}": {
      "get": {
        "description": "Get a location.",
        "tags": [
          "locations"
        ],
        "summary": "Get a location based on its uuid related to this key.",
        "operationId": "weaviate.locations.get",
        "parameters": [
          {
            "type": "string",
            "description": "Unique ID of the location.",
            "name": "locationId",
            "in": "path",
            "required": true
          }
        ],
        "responses": {
          "200": {
            "description": "Successful response.",
            "schema": {
              "$ref": "#/definitions/LocationGetResponse"
            }
          },
          "401": {
            "description": "Unauthorized or invalid credentials."
          },
          "403": {
            "description": "The used API-key has insufficient permissions."
          },
          "404": {
            "description": "Successful query result but no resource was found."
          },
          "501": {
            "description": "Not (yet) implemented."
          }
        },
        "x-available-in-mqtt": false
      },
      "put": {
        "description": "Updates an location.",
        "tags": [
          "locations"
        ],
        "summary": "Update a location based on its uuid related to this key.",
        "operationId": "weaviate.locations.update",
        "parameters": [
          {
            "name": "body",
            "in": "body",
            "required": true,
            "schema": {
              "$ref": "#/definitions/LocationUpdate"
            }
          },
          {
            "type": "string",
            "description": "Unique ID of the location.",
            "name": "locationId",
            "in": "path",
            "required": true
          }
        ],
        "responses": {
          "200": {
            "description": "Successful updated.",
            "schema": {
              "$ref": "#/definitions/LocationGetResponse"
            }
          },
          "401": {
            "description": "Unauthorized or invalid credentials."
          },
          "403": {
            "description": "The used API-key has insufficient permissions."
          },
          "404": {
            "description": "Successful query result but no resource was found."
          },
          "422": {
            "description": "Can not validate, check the body."
          },
          "501": {
            "description": "Not (yet) implemented."
          }
        },
        "x-available-in-mqtt": false
      },
      "delete": {
        "description": "Deletes an location.",
        "tags": [
          "locations"
        ],
        "summary": "Delete a location based on its uuid related to this key.",
        "operationId": "weaviate.locations.delete",
        "parameters": [
          {
            "type": "string",
            "format": "uuid",
            "name": "locationId",
            "in": "path",
            "required": true
          }
        ],
        "responses": {
          "204": {
            "description": "Successful deleted."
          },
          "401": {
            "description": "Unauthorized or invalid credentials."
          },
          "403": {
            "description": "The used API-key has insufficient permissions."
          },
          "404": {
            "description": "Successful query result but no resource was found."
          },
          "501": {
            "description": "Not (yet) implemented."
          }
        },
        "x-available-in-mqtt": false
      },
      "patch": {
        "description": "Updates an location. This method supports patch semantics.",
        "tags": [
          "locations"
        ],
        "summary": "Update a location based on its uuid (using patch semantics) related to this key.",
        "operationId": "weaviate.locations.patch",
        "parameters": [
          {
            "type": "string",
            "description": "Unique ID of the location.",
            "name": "locationId",
            "in": "path",
            "required": true
          },
          {
            "description": "JSONPatch document as defined by RFC 6902.",
            "name": "body",
            "in": "body",
            "required": true,
            "schema": {
              "type": "array",
              "items": {
                "$ref": "#/definitions/PatchDocument"
              }
            }
          }
        ],
        "responses": {
          "200": {
            "description": "Successful updated.",
            "schema": {
              "$ref": "#/definitions/LocationGetResponse"
            }
          },
          "400": {
            "description": "The patch-JSON is malformed."
          },
          "401": {
            "description": "Unauthorized or invalid credentials."
          },
          "403": {
            "description": "The used API-key has insufficient permissions."
          },
          "404": {
            "description": "Successful query result but no resource was found."
          },
          "422": {
            "description": "The patch-JSON is valid but unprocessable."
          },
          "501": {
            "description": "Not (yet) implemented."
          }
        },
        "x-available-in-mqtt": false
      }
    },
    "/thingTemplates": {
      "get": {
        "description": "Lists all model manifests.",
        "tags": [
          "thingTemplates"
        ],
        "summary": "Get a list of thing template related to this key.",
        "operationId": "weaviate.thingTemplates.list",
        "parameters": [
          {
            "$ref": "#/parameters/CommonMaxResultsParameterQuery"
          },
          {
            "$ref": "#/parameters/CommonPageParameterQuery"
          }
        ],
        "responses": {
          "200": {
            "description": "Successful response.",
            "schema": {
              "$ref": "#/definitions/ThingTemplatesListResponse"
            }
          },
          "401": {
            "description": "Unauthorized or invalid credentials."
          },
          "403": {
            "description": "The used API-key has insufficient permissions."
          },
          "404": {
            "description": "Successful query result but no resource was found."
          },
          "501": {
            "description": "Not (yet) implemented."
          }
        },
        "x-available-in-mqtt": false
      },
      "post": {
        "description": "Creates a new thing template manifest.",
        "tags": [
          "thingTemplates"
        ],
        "summary": "Create a new thing template related to this key.",
        "operationId": "weaviate.thingTemplates.create",
        "parameters": [
          {
            "name": "body",
            "in": "body",
            "required": true,
            "schema": {
              "$ref": "#/definitions/ThingTemplate"
            }
          }
        ],
        "responses": {
          "202": {
            "description": "Successfully received.",
            "schema": {
              "$ref": "#/definitions/ThingTemplateGetResponse"
            }
          },
          "401": {
            "description": "Unauthorized or invalid credentials."
          },
          "403": {
            "description": "The used API-key has insufficient permissions."
          },
          "422": {
            "description": "Can not validate, check the body."
          },
          "501": {
            "description": "Not (yet) implemented."
          }
        },
        "x-available-in-mqtt": false
      }
    },
    "/thingTemplates/{thingTemplateId}": {
      "get": {
        "description": "Returns a particular model manifest.",
        "tags": [
          "thingTemplates"
        ],
        "summary": "Get a thing template based on its uuid related to this key.",
        "operationId": "weaviate.thingTemplates.get",
        "parameters": [
          {
            "type": "string",
            "description": "Unique ID of the model manifest.",
            "name": "thingTemplateId",
            "in": "path",
            "required": true
          }
        ],
        "responses": {
          "200": {
            "description": "Successful response.",
            "schema": {
              "$ref": "#/definitions/ThingTemplateGetResponse"
            }
          },
          "401": {
            "description": "Unauthorized or invalid credentials."
          },
          "403": {
            "description": "The used API-key has insufficient permissions."
          },
          "404": {
            "description": "Successful query result but no resource was found."
          },
          "501": {
            "description": "Not (yet) implemented."
          }
        },
        "x-available-in-mqtt": false
      },
      "put": {
        "description": "Updates a particular model manifest.",
        "tags": [
          "thingTemplates"
        ],
        "summary": "Update thing template based on its uuid related to this key.",
        "operationId": "weaviate.thingTemplates.update",
        "parameters": [
          {
            "type": "string",
            "description": "Unique ID of the model manifest.",
            "name": "thingTemplateId",
            "in": "path",
            "required": true
          },
          {
            "name": "body",
            "in": "body",
            "required": true,
            "schema": {
              "$ref": "#/definitions/ThingTemplate"
            }
          }
        ],
        "responses": {
          "200": {
            "description": "Successful updated.",
            "schema": {
              "$ref": "#/definitions/ThingTemplateGetResponse"
            }
          },
          "401": {
            "description": "Unauthorized or invalid credentials."
          },
          "403": {
            "description": "The used API-key has insufficient permissions."
          },
          "404": {
            "description": "Successful query result but no resource was found."
          },
          "422": {
            "description": "Can not validate, check the body."
          },
          "501": {
            "description": "Not (yet) implemented."
          }
        },
        "x-available-in-mqtt": false
      },
      "delete": {
        "description": "Deletes a particular model manifest.",
        "tags": [
          "thingTemplates"
        ],
        "summary": "Delete a thing template based on its uuid related to this key.",
        "operationId": "weaviate.thingTemplates.delete",
        "parameters": [
          {
            "type": "string",
            "format": "uuid",
            "description": "Unique ID of the model manifest.",
            "name": "thingTemplateId",
            "in": "path",
            "required": true
          }
        ],
        "responses": {
          "204": {
            "description": "Successful deleted."
          },
          "401": {
            "description": "Unauthorized or invalid credentials."
          },
          "403": {
            "description": "The used API-key has insufficient permissions."
          },
          "404": {
            "description": "Successful query result but no resource was found."
          },
          "501": {
            "description": "Not (yet) implemented."
          }
        },
        "x-available-in-mqtt": false
      },
      "patch": {
        "description": "Updates a particular model manifest.",
        "tags": [
          "thingTemplates"
        ],
        "summary": "Update a thing template based on its uuid (using patch sematics) related to this key.",
        "operationId": "weaviate.thingTemplates.patch",
        "parameters": [
          {
            "type": "string",
            "description": "Unique ID of the model manifest.",
            "name": "thingTemplateId",
            "in": "path",
            "required": true
          },
          {
            "description": "JSONPatch document as defined by RFC 6902.",
            "name": "body",
            "in": "body",
            "required": true,
            "schema": {
              "type": "array",
              "items": {
                "$ref": "#/definitions/PatchDocument"
              }
            }
          }
        ],
        "responses": {
          "200": {
            "description": "Successful updated.",
            "schema": {
              "$ref": "#/definitions/ThingTemplateGetResponse"
            }
          },
          "400": {
            "description": "The patch-JSON is malformed."
          },
          "401": {
            "description": "Unauthorized or invalid credentials."
          },
          "403": {
            "description": "The used API-key has insufficient permissions."
          },
          "404": {
            "description": "Successful query result but no resource was found."
          },
          "422": {
            "description": "The patch-JSON is valid but unprocessable."
          },
          "501": {
            "description": "Not (yet) implemented."
          }
        },
        "x-available-in-mqtt": false
      }
    },
    "/things": {
      "get": {
        "description": "Lists all things user has access to.",
        "tags": [
          "things"
        ],
        "summary": "Get a list of things related to this key.",
        "operationId": "weaviate.things.list",
        "parameters": [
          {
            "$ref": "#/parameters/CommonMaxResultsParameterQuery"
          },
          {
            "$ref": "#/parameters/CommonPageParameterQuery"
          }
        ],
        "responses": {
          "200": {
            "description": "Successful response.",
            "schema": {
              "$ref": "#/definitions/ThingsListResponse"
            }
          },
          "401": {
            "description": "Unauthorized or invalid credentials."
          },
          "403": {
            "description": "The used API-key has insufficient permissions."
          },
          "404": {
            "description": "Successful query result but no resource was found."
          },
          "501": {
            "description": "Not (yet) implemented."
          }
        },
        "x-available-in-mqtt": false
      },
      "post": {
        "description": "Registers a new thing. This method may be used only by aggregator things or adapters.",
        "tags": [
          "things"
        ],
        "summary": "Create a new thing based on a thing template related to this key.",
        "operationId": "weaviate.things.create",
        "parameters": [
          {
            "name": "body",
            "in": "body",
            "required": true,
            "schema": {
              "$ref": "#/definitions/ThingCreate"
            }
          }
        ],
        "responses": {
          "202": {
            "description": "Successfully received.",
            "schema": {
              "$ref": "#/definitions/ThingGetResponse"
            }
          },
          "401": {
            "description": "Unauthorized or invalid credentials."
          },
          "403": {
            "description": "The used API-key has insufficient permissions."
          },
          "422": {
            "description": "Can not validate, check the body."
          },
          "501": {
            "description": "Not (yet) implemented."
          }
        },
        "x-available-in-mqtt": false
      }
    },
    "/things/{thingId}": {
      "get": {
        "description": "Returns a particular thing data.",
        "tags": [
          "things"
        ],
        "summary": "Get a thing based on its uuid related to this key.",
        "operationId": "weaviate.things.get",
        "parameters": [
          {
            "type": "string",
            "description": "Unique ID of the thing.",
            "name": "thingId",
            "in": "path",
            "required": true
          }
        ],
        "responses": {
          "200": {
            "description": "Successful response.",
            "schema": {
              "$ref": "#/definitions/ThingGetResponse"
            }
          },
          "401": {
            "description": "Unauthorized or invalid credentials."
          },
          "403": {
            "description": "The used API-key has insufficient permissions."
          },
          "404": {
            "description": "Successful query result but no resource was found."
          },
          "501": {
            "description": "Not (yet) implemented."
          }
        },
        "x-available-in-mqtt": false
      },
      "put": {
        "description": "Updates a thing data.",
        "tags": [
          "things"
        ],
        "summary": "Update a thing based on its uuid related to this key.",
        "operationId": "weaviate.things.update",
        "parameters": [
          {
            "type": "string",
            "description": "Unique ID of the thing.",
            "name": "thingId",
            "in": "path",
            "required": true
          },
          {
            "name": "body",
            "in": "body",
            "required": true,
            "schema": {
              "$ref": "#/definitions/ThingUpdate"
            }
          }
        ],
        "responses": {
          "200": {
            "description": "Successful update.",
            "schema": {
              "$ref": "#/definitions/ThingGetResponse"
            }
          },
          "401": {
            "description": "Unauthorized or invalid credentials."
          },
          "403": {
            "description": "The used API-key has insufficient permissions."
          },
          "404": {
            "description": "Successful query result but no resource was found."
          },
          "422": {
            "description": "Can not validate, check the body."
          },
          "501": {
            "description": "Not (yet) implemented."
          }
        },
        "x-available-in-mqtt": false
      },
      "delete": {
        "description": "Deletes a thing from the system.",
        "tags": [
          "things"
        ],
        "summary": "Delete a thing based on its uuid related to this key.",
        "operationId": "weaviate.things.delete",
        "parameters": [
          {
            "type": "string",
            "format": "uuid",
            "description": "Unique ID of the thing.",
            "name": "thingId",
            "in": "path",
            "required": true
          }
        ],
        "responses": {
          "204": {
            "description": "Successful deleted."
          },
          "401": {
            "description": "Unauthorized or invalid credentials."
          },
          "403": {
            "description": "The used API-key has insufficient permissions."
          },
          "404": {
            "description": "Successful query result but no resource was found."
          },
          "501": {
            "description": "Not (yet) implemented."
          }
        },
        "x-available-in-mqtt": false
      },
      "patch": {
        "description": "Updates a thing data. This method supports patch semantics.",
        "tags": [
          "things"
        ],
        "summary": "Update a thing based on its uuid (using patch semantics) related to this key.",
        "operationId": "weaviate.things.patch",
        "parameters": [
          {
            "type": "string",
            "description": "Unique ID of the thing.",
            "name": "thingId",
            "in": "path",
            "required": true
          },
          {
            "description": "JSONPatch document as defined by RFC 6902.",
            "name": "body",
            "in": "body",
            "required": true,
            "schema": {
              "type": "array",
              "items": {
                "$ref": "#/definitions/PatchDocument"
              }
            }
          }
        ],
        "responses": {
          "200": {
            "description": "Successful update.",
            "schema": {
              "$ref": "#/definitions/ThingGetResponse"
            }
          },
          "400": {
            "description": "The patch-JSON is malformed."
          },
          "401": {
            "description": "Unauthorized or invalid credentials."
          },
          "403": {
            "description": "The used API-key has insufficient permissions."
          },
          "404": {
            "description": "Successful query result but no resource was found."
          },
          "422": {
            "description": "The patch-JSON is valid but unprocessable."
          },
          "501": {
            "description": "Not (yet) implemented."
          }
        },
        "x-available-in-mqtt": false
      }
    },
    "/things/{thingId}/events": {
      "get": {
        "description": "Lists events.",
        "tags": [
          "events"
        ],
        "summary": "Get a list of events based on a thing's uuid (also available as MQTT channel) related to this key.",
        "operationId": "weaviate.things.events.list",
        "parameters": [
          {
            "type": "string",
            "format": "uuid",
            "description": "Unique ID of the thing.",
            "name": "thingId",
            "in": "path",
            "required": true
          },
          {
            "$ref": "#/parameters/CommonMaxResultsParameterQuery"
          },
          {
            "$ref": "#/parameters/CommonPageParameterQuery"
          }
        ],
        "responses": {
          "200": {
            "description": "Successful response.",
            "schema": {
              "$ref": "#/definitions/EventsListResponse"
            }
          },
          "401": {
            "description": "Unauthorized or invalid credentials."
          },
          "403": {
            "description": "The used API-key has insufficient permissions."
          },
          "404": {
            "description": "Successful query result but no resource was found."
          },
          "501": {
            "description": "Not (yet) implemented."
          }
        },
        "x-available-in-mqtt": true
      },
      "post": {
        "description": "Create event.",
        "tags": [
          "events"
        ],
        "summary": "Create events for a thing (also available as MQTT channel) related to this key.",
        "operationId": "weaviate.things.events.create",
        "parameters": [
          {
            "type": "string",
            "description": "Unique ID of the thing.",
            "name": "thingId",
            "in": "path",
            "required": true
          },
          {
            "name": "body",
            "in": "body",
            "required": true,
            "schema": {
              "$ref": "#/definitions/EventCreate"
            }
          }
        ],
        "responses": {
          "202": {
            "description": "Successfully received.",
            "schema": {
              "$ref": "#/definitions/EventGetResponse"
            }
          },
          "401": {
            "description": "Unauthorized or invalid credentials."
          },
          "403": {
            "description": "The used API-key has insufficient permissions."
          },
          "422": {
            "description": "Can not validate, check the body."
          },
          "501": {
            "description": "Not (yet) implemented."
          }
        },
        "x-available-in-mqtt": false
      }
    }
  },
  "definitions": {
    "Command": {
      "type": "object",
      "properties": {
        "creationTimeMs": {
          "description": "Timestamp since epoch of a creation of a command.",
          "type": "integer",
          "format": "int64"
        },
        "creatorKey": {
          "description": "User that created the command (by key).",
          "type": "string"
        },
        "error": {
          "description": "Error descriptor.",
          "type": "object",
          "properties": {
            "arguments": {
              "description": "Positional error arguments used for error message formatting.",
              "type": "array",
              "items": {
                "type": "string"
              }
            },
            "code": {
              "description": "Error code.",
              "type": "string"
            },
            "message": {
              "description": "User-visible error message populated by the cloud based on command name and error code.",
              "type": "string"
            }
          }
        },
        "expirationTimeMs": {
          "description": "Timestamp since epoch of command expiration.",
          "type": "integer",
          "format": "int64"
        },
        "expirationTimeoutMs": {
          "description": "Expiration timeout for the command since its creation, 10 seconds min, 30 days max.",
          "type": "integer",
          "format": "int64"
        },
        "lastUpdateTimeMs": {
          "description": "Timestamp since epoch of last update made to the command.",
          "type": "integer",
          "format": "int64"
        },
        "name": {
          "description": "Full command name, including trait.",
          "type": "string"
        },
        "parameters": {
          "$ref": "#/definitions/CommandParameters"
        },
        "progress": {
          "$ref": "#/definitions/CommandProgress"
        },
        "results": {
          "$ref": "#/definitions/CommandResults"
        },
        "userAction": {
          "description": "Pending command state that is not acknowledged by the thing yet.",
          "type": "string"
        }
      }
    },
    "CommandCreate": {
      "type": "object",
      "allOf": [
        {
          "$ref": "#/definitions/Command"
        }
      ]
    },
    "CommandGetResponse": {
      "allOf": [
        {
          "$ref": "#/definitions/Command"
        },
        {
          "type": "object",
          "properties": {
            "id": {
              "type": "string",
              "format": "uuid"
            },
            "kind": {
              "description": "Identifies what kind of resource this is. Value: the fixed string \"weaviate#commandGetResponse\".",
              "type": "string",
              "default": "weaviate#commandGetResponse"
            }
          }
        }
      ]
    },
    "CommandParameters": {
      "description": "Parameters that are needed to execute the command. Will also be included in the events.",
      "allOf": [
        {
          "$ref": "#/definitions/JsonObject"
        }
      ]
    },
    "CommandProgress": {
      "description": "Progress of the command. Will also be included in the events.",
      "type": "string",
      "enum": [
        "aborted",
        "cancelled",
        "done",
        "error",
        "expired",
        "inProgress",
        "queued"
      ]
    },
    "CommandResults": {
      "description": "Results of the command. Will be published in events.",
      "allOf": [
        {
          "$ref": "#/definitions/JsonObject"
        }
      ]
    },
    "CommandUpdate": {
      "type": "object",
      "allOf": [
        {
          "$ref": "#/definitions/Command"
        }
      ]
    },
    "CommandsListResponse": {
      "description": "List of commands.",
      "type": "object",
      "properties": {
        "commands": {
          "description": "The actual list of commands.",
          "type": "array",
          "items": {
            "$ref": "#/definitions/CommandGetResponse"
          }
        },
        "kind": {
          "description": "Identifies what kind of resource this is. Value: the fixed string \"weaviate#commandsListResponse\".",
          "type": "string",
          "default": "weaviate#commandsListResponse"
        },
        "totalResults": {
          "description": "The total number of commands for the query. The number of items in a response may be smaller due to paging.",
          "type": "integer",
          "format": "int64"
        }
      }
    },
    "Event": {
      "type": "object",
      "properties": {
        "command": {
          "type": "object",
          "properties": {
            "commandParameters": {
              "$ref": "#/definitions/CommandParameters"
            }
          }
        },
        "commandId": {
          "description": "Command id.",
          "type": "string",
          "format": "uuid"
        },
        "thingId": {
          "description": "Thing id.",
          "type": "string",
          "format": "uuid"
        },
        "timeMs": {
          "description": "Time the event was generated in milliseconds since epoch UTC.",
          "type": "integer",
          "format": "int64"
        },
        "userkey": {
          "description": "User that caused the event (if applicable).",
          "type": "string"
        }
      }
    },
    "EventCreate": {
      "type": "object",
      "allOf": [
        {
          "$ref": "#/definitions/Event"
        }
      ]
    },
    "EventGetResponse": {
      "type": "object",
      "allOf": [
        {
          "$ref": "#/definitions/Event"
        },
        {
          "properties": {
            "commandProgress": {
              "$ref": "#/definitions/CommandProgress"
            },
            "commandResults": {
              "$ref": "#/definitions/CommandResults"
            },
            "eventId": {
              "description": "UUID of this event",
              "type": "string",
              "format": "uuid"
            },
            "id": {
              "description": "ID of the event.",
              "type": "string",
              "format": "uuid"
            },
            "kind": {
              "description": "Identifies what kind of resource this is. Value: the fixed string \"weaviate#eventGetResponse\".",
              "type": "string",
              "default": "weaviate#eventGetResponse"
            }
          }
        }
      ]
    },
    "EventValidate": {
      "type": "object",
      "allOf": [
        {
          "$ref": "#/definitions/EventCreate"
        }
      ]
    },
    "EventsListResponse": {
      "description": "List of events.",
      "type": "object",
      "properties": {
        "events": {
          "description": "The actual list of events in reverse chronological order.",
          "type": "array",
          "items": {
            "$ref": "#/definitions/EventGetResponse"
          }
        },
        "kind": {
          "description": "Identifies what kind of resource this is. Value: the fixed string \"weaviate#eventsListResponse\".",
          "type": "string",
          "default": "weaviate#eventsListResponse"
        },
        "totalResults": {
          "description": "The total number of events for the query. The number of items in a response may be smaller due to paging.",
          "type": "integer",
          "format": "int64"
        }
      }
    },
    "Group": {
      "description": "Group.",
      "type": "object",
      "properties": {
        "ids": {
          "description": "The items in the group.",
          "type": "array",
          "items": {
            "type": "string",
            "format": "uuid"
          }
        },
        "name": {
          "description": "Name of the group.",
          "type": "string"
        }
      }
    },
    "GroupCreate": {
      "type": "object",
      "allOf": [
        {
          "$ref": "#/definitions/Group"
        }
      ]
    },
    "GroupGetResponse": {
      "allOf": [
        {
          "$ref": "#/definitions/Group"
        },
        {
          "type": "object",
          "properties": {
            "id": {
              "description": "ID of the group.",
              "type": "string",
              "format": "uuid"
            },
            "kind": {
              "description": "Identifies what kind of resource this is. Value: the fixed string \"weaviate#groupGetResponse\".",
              "type": "string",
              "default": "weaviate#groupGetResponse"
            }
          }
        }
      ]
    },
    "GroupUpdate": {
      "type": "object",
      "allOf": [
        {
          "$ref": "#/definitions/Group"
        }
      ]
    },
    "GroupsListResponse": {
      "type": "object",
      "properties": {
        "groups": {
          "description": "The list of groups.",
          "type": "array",
          "items": {
            "$ref": "#/definitions/GroupGetResponse"
          }
        },
        "kind": {
          "description": "Identifies what kind of resource this is. Value: the fixed string \"weaviate#groupsListResponse\".",
          "type": "string",
          "default": "weaviate#groupsListResponse"
        },
        "totalResults": {
          "description": "The total number of groups for the query. The number of items in a response may be smaller due to paging.",
          "type": "integer",
          "format": "int64"
        }
      }
    },
    "JsonObject": {
      "description": "JSON object value.",
      "type": "object",
      "additionalProperties": {
        "$ref": "#/definitions/JsonValue"
      }
    },
    "JsonValue": {
      "description": "JSON value -- union over JSON value types."
    },
    "KeyChildren": {
      "properties": {
        "children": {
          "$ref": "#/definitions/JsonObject"
        }
      }
    },
    "KeyCreate": {
      "properties": {
        "delete": {
          "description": "Is user allowed to delete.",
          "type": "boolean"
        },
        "email": {
          "description": "Email associated with this account.",
          "type": "string"
        },
        "ipOrigin": {
          "description": "Origin of the IP using CIDR notation.",
          "type": "string"
        },
        "keyExpiresUnix": {
          "description": "Time as Unix timestamp that the key expires. Set to 0 for never.",
          "type": "number"
        },
        "read": {
          "description": "Is user allowed to read.",
          "type": "boolean"
        },
        "write": {
          "description": "Is user allowed to write.",
          "type": "boolean"
        }
      }
    },
    "KeyGetResponse": {
      "allOf": [
        {
          "$ref": "#/definitions/KeyCreate"
        },
        {
          "properties": {
            "id": {
              "description": "Id of the key.",
              "type": "string",
              "format": "uuid"
            },
            "key": {
              "description": "Key for user to use.",
              "type": "string"
            },
            "kind": {
              "description": "Identifies what kind of resource this is. Value: the fixed string \"weaviate#keyGetResponse\".",
              "type": "string",
              "default": "weaviate#keyGetResponse"
            },
            "parent": {
              "description": "Parent key. A parent allways has access to a child. Root key has parent value 0. Only a user with a root of 0 can set a root key.",
              "type": "string"
            }
          }
        }
      ]
    },
    "Location": {
      "description": "Location on the world (inspired by Google Maps).",
      "type": "object",
      "properties": {
        "address_components": {
          "description": "Address descriptor",
          "type": "array",
          "items": {
            "type": "object",
            "properties": {
              "long_name": {
                "description": "Location address long name",
                "type": "string"
              },
              "short_name": {
                "description": "Location address short name",
                "type": "string"
              },
              "types": {
                "description": "Address type from list.",
                "type": "array",
                "items": {
                  "$ref": "#/definitions/LocationsAddressTypes"
                }
              }
            }
          }
        },
        "formatted_address": {
          "description": "Natural representation of the address.",
          "type": "string"
        },
        "geometry": {
          "type": "object",
          "properties": {
            "location": {
              "description": "Location coordinates.",
              "type": "object",
              "properties": {
                "elevation": {
                  "description": "Elevation in meters (inspired by Elevation API).",
                  "type": "number",
                  "format": "float"
                },
                "lat": {
                  "description": "Location's latitude.",
                  "type": "number",
                  "format": "float"
                },
                "lng": {
                  "description": "Location's longitude.",
                  "type": "number",
                  "format": "float"
                },
                "resolution": {
                  "description": "The maximum distance between data points from which the elevation was interpolated, in meters (inspired by Elevation API).",
                  "type": "number",
                  "format": "float"
                }
              }
            },
            "location_type": {
              "description": "Location's type",
              "type": "string"
            },
            "viewport": {
              "description": "Viewport corners",
              "type": "object",
              "properties": {
                "northeast": {
                  "description": "Northeast corner coordinates.",
                  "type": "object",
                  "properties": {
                    "lat": {
                      "type": "number",
                      "format": "float"
                    },
                    "lng": {
                      "type": "number",
                      "format": "float"
                    }
                  }
                },
                "southwest": {
                  "description": "Southwest corner coordinates.",
                  "type": "object",
                  "properties": {
                    "lat": {
                      "type": "number",
                      "format": "float"
                    },
                    "lng": {
                      "type": "number",
                      "format": "float"
                    }
                  }
                }
              }
            }
          }
        },
        "groups": {
          "description": "The list of groups.",
          "type": "array",
          "items": {
            "$ref": "#/definitions/Group"
          }
        },
        "place_id": {
          "description": "The ID of the place corresponding the location (optional Google Maps place_id).",
          "type": "string"
        },
        "types": {
          "description": "Location type from list.",
          "type": "array",
          "items": {
            "$ref": "#/definitions/LocationsAddressTypes"
          }
        }
      }
    },
    "LocationCreate": {
      "description": "Create location in the world (inspired by Google Maps).",
      "type": "object",
      "allOf": [
        {
          "$ref": "#/definitions/Location"
        }
      ]
    },
    "LocationGetResponse": {
      "description": "Location on the world (inspired by Google Maps).",
      "allOf": [
        {
          "$ref": "#/definitions/Location"
        },
        {
          "type": "object",
          "properties": {
            "id": {
              "description": "ID of the location.",
              "type": "string",
              "format": "uuid"
            },
            "kind": {
              "description": "Identifies what kind of resource this is. Value: the fixed string \"weaviate#locationGetResponse\".",
              "type": "string",
              "default": "weaviate#locationGetResponse"
            }
          }
        }
      ]
    },
    "LocationUpdate": {
      "description": "Update location in the world (inspired by Google Maps).",
      "type": "object",
      "allOf": [
        {
          "$ref": "#/definitions/Location"
        }
      ]
    },
    "LocationsAddressTypes": {
      "type": "string",
      "enum": [
        "UNDEFINED",
        "accounting",
        "administrative_area_level_1",
        "administrative_area_level_2",
        "administrative_area_level_3",
        "administrative_area_level_4",
        "administrative_area_level_5",
        "airport",
        "amusement_park",
        "aquarium",
        "art_gallery",
        "atm",
        "bakery",
        "bank",
        "bar",
        "beauty_salon",
        "bicycle_store",
        "book_store",
        "bowling_alley",
        "bus_station",
        "cafe",
        "campground",
        "car_dealer",
        "car_rental",
        "car_repair",
        "car_wash",
        "casino",
        "cemetery",
        "church",
        "city_hall",
        "clothing_store",
        "colloquial_area",
        "convenience_store",
        "country",
        "courthouse",
        "dentist",
        "department_store",
        "doctor",
        "electrician",
        "electronics_store",
        "embassy",
        "establishment",
        "finance",
        "fire_station",
        "floor",
        "florist",
        "food",
        "funeral_home",
        "furniture_store",
        "gas_station",
        "general_contractor",
        "geocode",
        "grocery_or_supermarket",
        "gym",
        "hair_care",
        "hardware_store",
        "health",
        "hindu_temple",
        "home_goods_store",
        "hospital",
        "insurance_agency",
        "intersection",
        "jewelry_store",
        "laundry",
        "lawyer",
        "library",
        "liquor_store",
        "local_government_office",
        "locality",
        "locksmith",
        "lodging",
        "meal_delivery",
        "meal_takeaway",
        "mosque",
        "movie_rental",
        "movie_theater",
        "moving_company",
        "museum",
        "natural_feature",
        "neighborhood",
        "night_club",
        "painter",
        "park",
        "parking",
        "pet_store",
        "pharmacy",
        "physiotherapist",
        "place_of_worship",
        "plumber",
        "point_of_interest",
        "police",
        "political",
        "post_box",
        "post_office",
        "postal_code",
        "postal_code_prefix",
        "postal_code_suffix",
        "postal_town",
        "premise",
        "real_estate_agency",
        "restaurant",
        "roofing_contractor",
        "room",
        "route",
        "rv_park",
        "school",
        "shoe_store",
        "shopping_mall",
        "spa",
        "stadium",
        "storage",
        "store",
        "street_address",
        "street_number",
        "sublocality",
        "sublocality_level_1",
        "sublocality_level_2",
        "sublocality_level_3",
        "sublocality_level_4",
        "sublocality_level_5",
        "subpremise",
        "subway_station",
        "synagogue",
        "taxi_stand",
        "train_station",
        "transit_station",
        "travel_agency",
        "university",
        "veterinary_care",
        "zoo"
      ]
    },
    "LocationsListResponse": {
      "type": "object",
      "properties": {
        "kind": {
          "description": "Identifies what kind of resource this is. Value: the fixed string \"weaviate#locationsListResponse\".",
          "type": "string",
          "default": "weaviate#locationsListResponse"
        },
        "locations": {
          "description": "The list of locations.",
          "type": "array",
          "items": {
            "$ref": "#/definitions/LocationGetResponse"
          }
        },
        "totalResults": {
          "description": "The total number of locations for the query. The number of items in a response may be smaller due to paging.",
          "type": "integer",
          "format": "int64"
        }
      }
    },
    "PatchDocument": {
      "description": "A JSONPatch document as defined by RFC 6902.",
      "required": [
        "op",
        "path"
      ],
      "properties": {
        "from": {
          "description": "A string containing a JSON Pointer value.",
          "type": "string"
        },
        "op": {
          "description": "The operation to be performed.",
          "type": "string",
          "enum": [
            "add",
            "remove",
            "replace",
            "move",
            "copy",
            "test"
          ]
        },
        "path": {
          "description": "A JSON-Pointer.",
          "type": "string"
        },
        "value": {
          "description": "The value to be used within the operations.",
          "type": "object"
        }
      }
    },
    "Thing": {
      "allOf": [
        {
          "$ref": "#/definitions/ThingTemplate"
        },
        {
          "type": "object",
          "properties": {
            "connectionStatus": {
              "description": "Thing connection status.",
              "type": "string"
            },
            "creationTimeMs": {
              "description": "Timestamp of creation of this thing in milliseconds since epoch UTC.",
              "type": "integer",
              "format": "int64"
            },
            "description": {
              "description": "User readable description of this thing.",
              "type": "string"
            },
            "groups": {
              "description": "The list of groups.",
              "type": "array",
              "items": {
                "type": "string",
                "format": "uuid"
              }
            },
            "lastSeenTimeMs": {
              "description": "Timestamp of the last request from this thing in milliseconds since epoch UTC. Supported only for things with XMPP channel type.",
              "type": "integer",
              "format": "int64"
            },
            "lastUpdateTimeMs": {
              "description": "Timestamp of the last thing update in milliseconds since epoch UTC.",
              "type": "integer",
              "format": "int64"
            },
            "lastUseTimeMs": {
              "description": "Timestamp of the last thing usage in milliseconds since epoch UTC.",
              "type": "integer",
              "format": "int64"
            },
            "locationId": {
              "description": "ID of the location of this thing.",
              "type": "string",
              "format": "uuid"
            },
            "owner": {
              "description": "E-mail address of the thing owner.",
              "type": "string"
            },
            "serialNumber": {
              "description": "Serial number of a thing provided by its manufacturer.",
              "type": "string"
            },
            "tags": {
              "description": "Custom free-form manufacturer tags.",
              "type": "array",
              "items": {
                "type": "string"
              }
            },
            "thingTemplateId": {
              "description": "Model manifest ID of this thing.",
              "type": "string",
              "format": "uuid"
            }
          }
        }
      ]
    },
    "ThingCreate": {
      "type": "object",
      "properties": {
        "commandsId": {
          "type": "string",
          "format": "uuid"
        },
        "description": {
          "type": "string"
        },
        "groups": {
          "type": "string"
        },
        "locationId": {
          "type": "string",
          "format": "uuid"
        },
        "name": {
          "type": "string"
        },
        "owner": {
          "type": "string"
        },
        "serialNumber": {
          "type": "string"
        },
        "tags": {
          "type": "array"
        },
        "thingTemplateId": {
          "type": "string",
          "format": "uuid"
        }
      }
    },
    "ThingGetResponse": {
      "allOf": [
        {
          "$ref": "#/definitions/Thing"
        },
        {
          "type": "object",
          "properties": {
            "id": {
              "type": "string",
              "format": "uuid"
            },
            "kind": {
              "description": "Identifies what kind of resource this is. Value: the fixed string \"weaviate#thingGetResponse\".",
              "type": "string",
              "default": "weaviate#thingGetResponse"
            }
          }
        }
      ]
    },
    "ThingTemplate": {
      "type": "object",
      "properties": {
        "commandsIds": {
          "description": "The id of the commands that this device is able to execute.",
          "type": "array",
          "items": {
            "type": "string",
            "format": "uuid"
          }
        },
        "name": {
          "description": "Name of this thing provided by the manufacturer.",
          "type": "string"
        },
        "thingModelTemplate": {
          "description": "Thing model information provided by the model manifest of this thing.",
          "type": "object",
          "properties": {
            "modelName": {
              "description": "Thing model name.",
              "type": "string"
            },
            "oemAdditions": {
              "description": "OEM additions as key values",
              "type": "object",
              "additionalProperties": {
                "$ref": "#/definitions/JsonValue"
              }
            },
            "oemContact": {
              "description": "Contact information in URL format.",
              "type": "string",
              "format": "url"
            },
            "oemIcon": {
              "description": "Image of icon.",
              "type": "string",
              "format": "url"
            },
            "oemName": {
              "description": "Name of thing model manufacturer.",
              "type": "string"
            },
            "oemNumber": {
              "description": "Unique OEM oemNumber",
              "type": "string"
            }
          }
        }
      }
    },
    "ThingTemplateGetResponse": {
      "allOf": [
        {
          "$ref": "#/definitions/ThingTemplate"
        },
        {
          "type": "object",
          "properties": {
            "id": {
              "type": "string",
              "format": "uuid"
            },
            "kind": {
              "description": "Identifies what kind of resource this is. Value: the fixed string \"weaviate#thingTemplateGetResponse\".",
              "type": "string",
              "default": "weaviate#thingTemplateGetResponse"
            }
          }
        }
      ]
    },
    "ThingTemplatesListResponse": {
      "description": "List of model manifests.",
      "type": "object",
      "properties": {
        "kind": {
          "description": "Identifies what kind of resource this is. Value: the fixed string \"weaviate#thingTemplatesListResponse\".",
          "type": "string",
          "default": "weaviate#thingTemplatesListResponse"
        },
        "thingTemplates": {
          "description": "The actual list of model manifests.",
          "type": "array",
          "items": {
            "$ref": "#/definitions/ThingTemplateGetResponse"
          }
        },
        "totalResults": {
          "description": "The total number of model manifests for the query. The number of items in a response may be smaller due to paging.",
          "type": "integer",
          "format": "int64"
        }
      }
    },
    "ThingUpdate": {
      "type": "object",
      "allOf": [
        {
          "$ref": "#/definitions/Thing"
        }
      ]
    },
    "ThingsListResponse": {
      "description": "List of things.",
      "type": "object",
      "properties": {
        "kind": {
          "description": "Identifies what kind of resource this is. Value: the fixed string \"weaviate#thingsListResponse\".",
          "type": "string",
          "default": "weaviate#thingsListResponse"
        },
        "things": {
          "description": "The actual list of things.",
          "type": "array",
          "items": {
            "$ref": "#/definitions/ThingGetResponse"
          }
        },
        "totalResults": {
          "description": "The total number of things for the query. The number of items in a response may be smaller due to paging.",
          "type": "integer",
          "format": "int64"
        }
      }
    }
  },
  "parameters": {
    "CommonMaxResultsParameterQuery": {
      "type": "integer",
      "format": "int64",
      "description": "The maximum number of items to be returned per page.",
      "name": "maxResults",
      "in": "query"
    },
    "CommonPageParameterQuery": {
      "type": "integer",
      "format": "int64",
      "description": "The page number of the items to be returned.",
      "name": "page",
      "in": "query"
    }
  },
  "securityDefinitions": {
    "apiKey": {
      "type": "apiKey",
      "name": "X-API-KEY",
      "in": "header"
    }
  },
  "security": [
    {
      "apiKey": []
    }
  ],
  "tags": [
    {
      "name": "commands"
    },
    {
      "name": "events"
    },
    {
      "name": "groups"
    },
    {
      "name": "locations"
    },
    {
      "name": "keys"
    },
    {
      "name": "thingTemplates"
    },
    {
      "name": "things"
    }
  ],
  "externalDocs": {
    "url": "https://github.com/weaviate/weaviate"
  }
}`))
}
