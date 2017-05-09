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
    "description": "Lets you register, view and manage cloud ready devices.",
    "title": "Weaviate API",
    "contact": {
      "name": "Weaviate",
      "url": "https://github.com/weaviate/weaviate"
    },
    "version": "v1-alpha"
  },
  "basePath": "/weaviate/v1-alpha",
  "paths": {
    "/adapters": {
      "get": {
        "description": "Lists all adapters.",
        "tags": [
          "adapters"
        ],
        "operationId": "weaviate.adapters.list",
        "responses": {
          "200": {
            "description": "Successful response.",
            "schema": {
              "$ref": "#/definitions/AdaptersListResponse"
            }
          },
          "404": {
            "description": "Successful query result but no resource was found."
          },
          "501": {
            "description": "Not (yet) implemented."
          }
        }
      },
      "post": {
        "description": "Inserts adapter.",
        "tags": [
          "adapters"
        ],
        "operationId": "weaviate.adapters.insert",
        "parameters": [
          {
            "name": "body",
            "in": "body",
            "required": true,
            "schema": {
              "$ref": "#/definitions/Adapter"
            }
          }
        ],
        "responses": {
          "202": {
            "description": "Successfully received.",
            "schema": {
              "$ref": "#/definitions/Adapter"
            }
          },
          "501": {
            "description": "Not (yet) implemented."
          }
        }
      },
      "parameters": [
        {
          "$ref": "#/parameters/alt"
        },
        {
          "$ref": "#/parameters/fields"
        },
        {
          "$ref": "#/parameters/key"
        },
        {
          "$ref": "#/parameters/oauth_token"
        },
        {
          "$ref": "#/parameters/prettyPrint"
        },
        {
          "$ref": "#/parameters/quotaUser"
        },
        {
          "$ref": "#/parameters/userIp"
        }
      ]
    },
    "/adapters/{adapterId}": {
      "get": {
        "description": "Get an adapter.",
        "tags": [
          "adapters"
        ],
        "operationId": "weaviate.adapters.get",
        "parameters": [
          {
            "type": "string",
            "description": "Unique ID of the adapter.",
            "name": "adapterId",
            "in": "path",
            "required": true
          }
        ],
        "responses": {
          "200": {
            "description": "Successful response.",
            "schema": {
              "$ref": "#/definitions/Adapter"
            }
          },
          "404": {
            "description": "Successful query result but no resource was found."
          },
          "501": {
            "description": "Not (yet) implemented."
          }
        }
      },
      "put": {
        "description": "Updates an adapter.",
        "tags": [
          "adapters"
        ],
        "operationId": "weaviate.adapters.update",
        "parameters": [
          {
            "name": "body",
            "in": "body",
            "required": true,
            "schema": {
              "$ref": "#/definitions/Adapter"
            }
          },
          {
            "type": "string",
            "description": "Unique ID of the adapter.",
            "name": "adapterId",
            "in": "path",
            "required": true
          }
        ],
        "responses": {
          "200": {
            "description": "Successful updated.",
            "schema": {
              "$ref": "#/definitions/Adapter"
            }
          },
          "404": {
            "description": "Successful query result but no resource was found."
          },
          "501": {
            "description": "Not (yet) implemented."
          }
        }
      },
      "delete": {
        "description": "Deletes an adapter.",
        "tags": [
          "adapters"
        ],
        "operationId": "weaviate.adapters.delete",
        "parameters": [
          {
            "type": "string",
            "name": "adapterId",
            "in": "path",
            "required": true
          }
        ],
        "responses": {
          "204": {
            "description": "Successful deleted."
          },
          "404": {
            "description": "Successful query result but no resource was found."
          },
          "501": {
            "description": "Not (yet) implemented."
          }
        }
      },
      "patch": {
        "description": "Updates an adapter. This method supports patch semantics.",
        "tags": [
          "adapters"
        ],
        "operationId": "weaviate.adapters.patch",
        "parameters": [
          {
            "type": "string",
            "description": "Unique ID of the adapter.",
            "name": "adapterId",
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
              "$ref": "#/definitions/Adapter"
            }
          },
          "404": {
            "description": "Successful query result but no resource was found."
          },
          "501": {
            "description": "Not (yet) implemented."
          }
        }
      },
      "parameters": [
        {
          "$ref": "#/parameters/alt"
        },
        {
          "$ref": "#/parameters/fields"
        },
        {
          "$ref": "#/parameters/key"
        },
        {
          "$ref": "#/parameters/oauth_token"
        },
        {
          "$ref": "#/parameters/prettyPrint"
        },
        {
          "$ref": "#/parameters/quotaUser"
        },
        {
          "$ref": "#/parameters/userIp"
        }
      ]
    },
    "/commands": {
      "get": {
        "description": "Lists all commands in reverse order of creation.",
        "tags": [
          "commands"
        ],
        "operationId": "weaviate.commands.list",
        "parameters": [
          {
            "type": "string",
            "description": "List all the commands issued by the user. Special value 'me' can be used to list by the current user.",
            "name": "byUser",
            "in": "query"
          },
          {
            "type": "string",
            "description": "Specifies the language code that should be used for text values in the API response.",
            "name": "hl",
            "in": "query"
          },
          {
            "type": "integer",
            "name": "maxResults",
            "in": "query"
          },
          {
            "type": "integer",
            "name": "startIndex",
            "in": "query"
          },
          {
            "enum": [
              "aborted",
              "cancelled",
              "done",
              "error",
              "expired",
              "inProgress",
              "queued"
            ],
            "type": "string",
            "description": "Command state.",
            "name": "state",
            "in": "query"
          },
          {
            "type": "string",
            "name": "token",
            "in": "query"
          }
        ],
        "responses": {
          "200": {
            "description": "Successful response.",
            "schema": {
              "$ref": "#/definitions/CommandsListResponse"
            }
          },
          "404": {
            "description": "Successful query result but no resource was found."
          },
          "501": {
            "description": "Not (yet) implemented."
          }
        }
      },
      "post": {
        "description": "Inserts and sends a new command.",
        "tags": [
          "commands"
        ],
        "operationId": "weaviate.commands.insert",
        "parameters": [
          {
            "type": "string",
            "description": "ID of the command that was sent before this command. Use this to ensure the order of commands.",
            "name": "executeAfter",
            "in": "query"
          },
          {
            "type": "string",
            "description": "Specifies the language code that should be used for text values in the API response.",
            "name": "hl",
            "in": "query"
          },
          {
            "maximum": 25000,
            "type": "integer",
            "format": "int64",
            "description": "Number of milliseconds to wait for device response before returning.",
            "name": "responseAwaitMs",
            "in": "query"
          },
          {
            "name": "body",
            "in": "body",
            "required": true,
            "schema": {
              "$ref": "#/definitions/Command"
            }
          }
        ],
        "responses": {
          "202": {
            "description": "Successfully received.",
            "schema": {
              "$ref": "#/definitions/Command"
            }
          },
          "501": {
            "description": "Not (yet) implemented."
          }
        }
      },
      "parameters": [
        {
          "$ref": "#/parameters/alt"
        },
        {
          "$ref": "#/parameters/fields"
        },
        {
          "$ref": "#/parameters/key"
        },
        {
          "$ref": "#/parameters/oauth_token"
        },
        {
          "$ref": "#/parameters/prettyPrint"
        },
        {
          "$ref": "#/parameters/quotaUser"
        },
        {
          "$ref": "#/parameters/userIp"
        }
      ]
    },
    "/commands/queue": {
      "get": {
        "description": "Returns all queued commands that device is supposed to execute. This method may be used only by devices.",
        "tags": [
          "commands"
        ],
        "operationId": "weaviate.commands.getQueue",
        "parameters": [
          {
            "type": "string",
            "description": "Specifies the language code that should be used for text values in the API response.",
            "name": "hl",
            "in": "query"
          },
          {
            "type": "integer",
            "name": "maxResults",
            "in": "query"
          },
          {
            "type": "integer",
            "name": "startIndex",
            "in": "query"
          },
          {
            "type": "string",
            "name": "token",
            "in": "query"
          }
        ],
        "responses": {
          "200": {
            "description": "Successful response.",
            "schema": {
              "$ref": "#/definitions/CommandsQueueResponse"
            }
          },
          "404": {
            "description": "Successful query result but no resource was found."
          },
          "501": {
            "description": "Not (yet) implemented."
          }
        }
      },
      "parameters": [
        {
          "$ref": "#/parameters/alt"
        },
        {
          "$ref": "#/parameters/fields"
        },
        {
          "$ref": "#/parameters/key"
        },
        {
          "$ref": "#/parameters/oauth_token"
        },
        {
          "$ref": "#/parameters/prettyPrint"
        },
        {
          "$ref": "#/parameters/quotaUser"
        },
        {
          "$ref": "#/parameters/userIp"
        }
      ]
    },
    "/commands/{commandId}": {
      "get": {
        "description": "Returns a particular command.",
        "tags": [
          "commands"
        ],
        "operationId": "weaviate.commands.get",
        "parameters": [
          {
            "type": "string",
            "description": "Unique command ID.",
            "name": "commandId",
            "in": "path",
            "required": true
          },
          {
            "type": "string",
            "description": "Path to the blob inside the command, for now only two values are supported: \"parameters\" and \"results\".",
            "name": "attachmentPath",
            "in": "query"
          },
          {
            "type": "string",
            "description": "Specifies the language code that should be used for text values in the API response.",
            "name": "hl",
            "in": "query"
          }
        ],
        "responses": {
          "200": {
            "description": "Successful response.",
            "schema": {
              "$ref": "#/definitions/Command"
            }
          },
          "404": {
            "description": "Successful query result but no resource was found."
          },
          "501": {
            "description": "Not (yet) implemented."
          }
        }
      },
      "put": {
        "description": "Updates a command. This method may be used only by devices.",
        "tags": [
          "commands"
        ],
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
            "type": "string",
            "description": "Specifies the language code that should be used for text values in the API response.",
            "name": "hl",
            "in": "query"
          },
          {
            "name": "body",
            "in": "body",
            "required": true,
            "schema": {
              "$ref": "#/definitions/Command"
            }
          }
        ],
        "responses": {
          "200": {
            "description": "Successful updated.",
            "schema": {
              "$ref": "#/definitions/Command"
            }
          },
          "404": {
            "description": "Successful query result but no resource was found."
          },
          "501": {
            "description": "Not (yet) implemented."
          }
        }
      },
      "delete": {
        "description": "Deletes a command.",
        "tags": [
          "commands"
        ],
        "operationId": "weaviate.commands.delete",
        "parameters": [
          {
            "type": "string",
            "description": "Unique command ID.",
            "name": "commandId",
            "in": "path",
            "required": true
          },
          {
            "type": "string",
            "description": "Specifies the language code that should be used for text values in the API response.",
            "name": "hl",
            "in": "query"
          }
        ],
        "responses": {
          "204": {
            "description": "Successful deleted."
          },
          "404": {
            "description": "Successful query result but no resource was found."
          },
          "501": {
            "description": "Not (yet) implemented."
          }
        }
      },
      "patch": {
        "description": "Updates a command. This method may be used only by devices. This method supports patch semantics.",
        "tags": [
          "commands"
        ],
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
          },
          {
            "type": "string",
            "description": "Specifies the language code that should be used for text values in the API response.",
            "name": "hl",
            "in": "query"
          }
        ],
        "responses": {
          "200": {
            "description": "Successful updated.",
            "schema": {
              "$ref": "#/definitions/Command"
            }
          },
          "404": {
            "description": "Successful query result but no resource was found."
          },
          "501": {
            "description": "Not (yet) implemented."
          }
        }
      },
      "parameters": [
        {
          "$ref": "#/parameters/alt"
        },
        {
          "$ref": "#/parameters/fields"
        },
        {
          "$ref": "#/parameters/key"
        },
        {
          "$ref": "#/parameters/oauth_token"
        },
        {
          "$ref": "#/parameters/prettyPrint"
        },
        {
          "$ref": "#/parameters/quotaUser"
        },
        {
          "$ref": "#/parameters/userIp"
        }
      ]
    },
    "/devices": {
      "get": {
        "description": "Lists all devices user has access to.",
        "tags": [
          "devices"
        ],
        "operationId": "weaviate.devices.list",
        "parameters": [
          {
            "type": "string",
            "description": "Device description.",
            "name": "descriptionSubstring",
            "in": "query"
          },
          {
            "enum": [
              "acHeating",
              "accessPoint",
              "adapterGateway",
              "aggregator",
              "camera",
              "developmentBoard",
              "fan",
              "light",
              "lock",
              "outlet",
              "printer",
              "scanner",
              "speaker",
              "storage",
              "switch",
              "toy",
              "tv",
              "unknownDeviceKind",
              "vendor",
              "video"
            ],
            "type": "string",
            "description": "Device kind.",
            "name": "deviceKind",
            "in": "query"
          },
          {
            "type": "string",
            "description": "Device display name. Deprecated, use 'nameSubstring' instead.",
            "name": "displayNameSubstring",
            "in": "query"
          },
          {
            "type": "string",
            "description": "Specifies the language code that should be used for text values in the API response.",
            "name": "hl",
            "in": "query"
          },
          {
            "type": "integer",
            "name": "maxResults",
            "in": "query"
          },
          {
            "type": "string",
            "description": "Device's location.",
            "name": "locationId",
            "in": "query"
          },
          {
            "type": "string",
            "description": "Device model manifest.",
            "name": "modelManifestId",
            "in": "query"
          },
          {
            "type": "string",
            "description": "Device name.",
            "name": "nameSubstring",
            "in": "query"
          },
          {
            "enum": [
              "manager",
              "owner",
              "robot",
              "user",
              "viewer"
            ],
            "type": "string",
            "description": "Access role to the device.",
            "name": "role",
            "in": "query"
          },
          {
            "type": "integer",
            "name": "startIndex",
            "in": "query"
          },
          {
            "type": "string",
            "description": "Device system name. Deprecated, use 'nameSubstring' instead.",
            "name": "systemNameSubstring",
            "in": "query"
          },
          {
            "type": "string",
            "name": "token",
            "in": "query"
          }
        ],
        "responses": {
          "200": {
            "description": "Successful response.",
            "schema": {
              "$ref": "#/definitions/DevicesListResponse"
            }
          },
          "404": {
            "description": "Successful query result but no resource was found."
          },
          "501": {
            "description": "Not (yet) implemented."
          }
        }
      },
      "post": {
        "description": "Registers a new device. This method may be used only by aggregator devices or adapters.",
        "tags": [
          "devices"
        ],
        "operationId": "weaviate.devices.insert",
        "parameters": [
          {
            "type": "string",
            "description": "ID of the adapter activation that this device belongs to, if any.",
            "name": "adapterActivationId",
            "in": "query"
          },
          {
            "type": "string",
            "description": "Specifies the language code that should be used for text values in the API response.",
            "name": "hl",
            "in": "query"
          },
          {
            "name": "body",
            "in": "body",
            "required": true,
            "schema": {
              "$ref": "#/definitions/Device"
            }
          }
        ],
        "responses": {
          "202": {
            "description": "Successfully received.",
            "schema": {
              "$ref": "#/definitions/Device"
            }
          },
          "501": {
            "description": "Not (yet) implemented."
          }
        }
      },
      "parameters": [
        {
          "$ref": "#/parameters/alt"
        },
        {
          "$ref": "#/parameters/fields"
        },
        {
          "$ref": "#/parameters/key"
        },
        {
          "$ref": "#/parameters/oauth_token"
        },
        {
          "$ref": "#/parameters/prettyPrint"
        },
        {
          "$ref": "#/parameters/quotaUser"
        },
        {
          "$ref": "#/parameters/userIp"
        }
      ]
    },
    "/devices/{deviceId}": {
      "get": {
        "description": "Returns a particular device data.",
        "tags": [
          "devices"
        ],
        "operationId": "weaviate.devices.get",
        "parameters": [
          {
            "type": "string",
            "description": "Unique ID of the device.",
            "name": "deviceId",
            "in": "path",
            "required": true
          },
          {
            "type": "string",
            "description": "Specifies the language code that should be used for text values in the API response.",
            "name": "hl",
            "in": "query"
          },
          {
            "enum": [
              "full",
              "noUserInfo"
            ],
            "type": "string",
            "description": "Projection controls which fields of the Device resource are returned.",
            "name": "projection",
            "in": "query"
          }
        ],
        "responses": {
          "200": {
            "description": "Successful response.",
            "schema": {
              "$ref": "#/definitions/Device"
            }
          },
          "404": {
            "description": "Successful query result but no resource was found."
          },
          "501": {
            "description": "Not (yet) implemented."
          }
        }
      },
      "put": {
        "description": "Updates a device data.",
        "tags": [
          "devices"
        ],
        "operationId": "weaviate.devices.update",
        "parameters": [
          {
            "type": "string",
            "description": "Unique ID of the device.",
            "name": "deviceId",
            "in": "path",
            "required": true
          },
          {
            "type": "string",
            "description": "Specifies the language code that should be used for text values in the API response.",
            "name": "hl",
            "in": "query"
          },
          {
            "type": "string",
            "description": "Previous last update time in device data. Optionally set this parameter to ensure an update call does not overwrite newer data.",
            "name": "lastUpdateTimeMs",
            "in": "query"
          },
          {
            "name": "body",
            "in": "body",
            "required": true,
            "schema": {
              "$ref": "#/definitions/Device"
            }
          }
        ],
        "responses": {
          "200": {
            "description": "Successful update.",
            "schema": {
              "$ref": "#/definitions/Device"
            }
          },
          "404": {
            "description": "Successful query result but no resource was found."
          },
          "501": {
            "description": "Not (yet) implemented."
          }
        }
      },
      "delete": {
        "description": "Deletes a device from the system.",
        "tags": [
          "devices"
        ],
        "operationId": "weaviate.devices.delete",
        "parameters": [
          {
            "type": "string",
            "description": "Unique ID of the device.",
            "name": "deviceId",
            "in": "path",
            "required": true
          },
          {
            "type": "string",
            "description": "Specifies the language code that should be used for text values in the API response.",
            "name": "hl",
            "in": "query"
          }
        ],
        "responses": {
          "204": {
            "description": "Successful deleted."
          },
          "404": {
            "description": "Successful query result but no resource was found."
          },
          "501": {
            "description": "Not (yet) implemented."
          }
        }
      },
      "patch": {
        "description": "Updates a device data. This method supports patch semantics.",
        "tags": [
          "devices"
        ],
        "operationId": "weaviate.devices.patch",
        "parameters": [
          {
            "type": "string",
            "description": "Unique ID of the device.",
            "name": "deviceId",
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
          },
          {
            "type": "string",
            "description": "Specifies the language code that should be used for text values in the API response.",
            "name": "hl",
            "in": "query"
          },
          {
            "type": "string",
            "description": "Previous last update time in device data. Optionally set this parameter to ensure an update call does not overwrite newer data.",
            "name": "lastUpdateTimeMs",
            "in": "query"
          }
        ],
        "responses": {
          "200": {
            "description": "Successful update.",
            "schema": {
              "$ref": "#/definitions/Device"
            }
          },
          "404": {
            "description": "Successful query result but no resource was found."
          },
          "501": {
            "description": "Not (yet) implemented."
          }
        }
      },
      "parameters": [
        {
          "$ref": "#/parameters/alt"
        },
        {
          "$ref": "#/parameters/fields"
        },
        {
          "$ref": "#/parameters/key"
        },
        {
          "$ref": "#/parameters/oauth_token"
        },
        {
          "$ref": "#/parameters/prettyPrint"
        },
        {
          "$ref": "#/parameters/quotaUser"
        },
        {
          "$ref": "#/parameters/userIp"
        }
      ]
    },
    "/devices/{deviceId}/aclEntries": {
      "get": {
        "description": "Lists ACL entries.",
        "tags": [
          "aclEntries"
        ],
        "operationId": "weaviate.aclEntries.list",
        "parameters": [
          {
            "type": "string",
            "description": "ID of the device to use.",
            "name": "deviceId",
            "in": "path",
            "required": true
          },
          {
            "type": "string",
            "description": "Specifies the language code that should be used for text values in the API response.",
            "name": "hl",
            "in": "query"
          },
          {
            "type": "integer",
            "name": "maxResults",
            "in": "query"
          },
          {
            "type": "integer",
            "name": "startIndex",
            "in": "query"
          },
          {
            "type": "string",
            "name": "token",
            "in": "query"
          }
        ],
        "responses": {
          "200": {
            "description": "Successful response.",
            "schema": {
              "$ref": "#/definitions/AclEntriesListResponse"
            }
          },
          "404": {
            "description": "Successful query result but no resource was found."
          },
          "501": {
            "description": "Not (yet) implemented."
          }
        }
      },
      "post": {
        "description": "Inserts a new ACL entry.",
        "tags": [
          "aclEntries"
        ],
        "operationId": "weaviate.aclEntries.insert",
        "parameters": [
          {
            "type": "string",
            "description": "ID of the device to use.",
            "name": "deviceId",
            "in": "path",
            "required": true
          },
          {
            "type": "string",
            "description": "Specifies the language code that should be used for text values in the API response.",
            "name": "hl",
            "in": "query"
          },
          {
            "name": "body",
            "in": "body",
            "required": true,
            "schema": {
              "$ref": "#/definitions/AclEntry"
            }
          }
        ],
        "responses": {
          "202": {
            "description": "Successfully received.",
            "schema": {
              "$ref": "#/definitions/AclEntry"
            }
          },
          "501": {
            "description": "Not (yet) implemented."
          }
        }
      },
      "parameters": [
        {
          "$ref": "#/parameters/alt"
        },
        {
          "$ref": "#/parameters/fields"
        },
        {
          "$ref": "#/parameters/key"
        },
        {
          "$ref": "#/parameters/oauth_token"
        },
        {
          "$ref": "#/parameters/prettyPrint"
        },
        {
          "$ref": "#/parameters/quotaUser"
        },
        {
          "$ref": "#/parameters/userIp"
        }
      ]
    },
    "/devices/{deviceId}/aclEntries/{aclEntryId}": {
      "get": {
        "description": "Returns the requested ACL entry.",
        "tags": [
          "aclEntries"
        ],
        "operationId": "weaviate.aclEntries.get",
        "parameters": [
          {
            "type": "string",
            "description": "ID of the device to use.",
            "name": "deviceId",
            "in": "path",
            "required": true
          },
          {
            "type": "string",
            "description": "Unique ACL entry ID.",
            "name": "aclEntryId",
            "in": "path",
            "required": true
          },
          {
            "type": "string",
            "description": "Specifies the language code that should be used for text values in the API response.",
            "name": "hl",
            "in": "query"
          }
        ],
        "responses": {
          "200": {
            "description": "Successful response.",
            "schema": {
              "$ref": "#/definitions/AclEntry"
            }
          },
          "404": {
            "description": "Successful query result but no resource was found."
          },
          "501": {
            "description": "Not (yet) implemented."
          }
        }
      },
      "put": {
        "description": "Updates an ACL entry.",
        "tags": [
          "aclEntries"
        ],
        "operationId": "weaviate.aclEntries.update",
        "parameters": [
          {
            "type": "string",
            "description": "ID of the device to use.",
            "name": "deviceId",
            "in": "path",
            "required": true
          },
          {
            "type": "string",
            "description": "Unique ACL entry ID.",
            "name": "aclEntryId",
            "in": "path",
            "required": true
          },
          {
            "type": "string",
            "description": "Specifies the language code that should be used for text values in the API response.",
            "name": "hl",
            "in": "query"
          },
          {
            "name": "body",
            "in": "body",
            "required": true,
            "schema": {
              "$ref": "#/definitions/AclEntry"
            }
          }
        ],
        "responses": {
          "200": {
            "description": "Successful updated.",
            "schema": {
              "$ref": "#/definitions/AclEntry"
            }
          },
          "404": {
            "description": "Successful query result but no resource was found."
          },
          "501": {
            "description": "Not (yet) implemented."
          }
        }
      },
      "delete": {
        "description": "Deletes an ACL entry.",
        "tags": [
          "aclEntries"
        ],
        "operationId": "weaviate.aclEntries.delete",
        "parameters": [
          {
            "type": "string",
            "description": "ID of the device to use.",
            "name": "deviceId",
            "in": "path",
            "required": true
          },
          {
            "type": "string",
            "description": "Unique ACL entry ID.",
            "name": "aclEntryId",
            "in": "path",
            "required": true
          }
        ],
        "responses": {
          "204": {
            "description": "Successful deleted."
          },
          "404": {
            "description": "Successful query result but no resource was found."
          },
          "501": {
            "description": "Not (yet) implemented."
          }
        }
      },
      "patch": {
        "description": "Updates an ACL entry. This method supports patch semantics.",
        "tags": [
          "aclEntries"
        ],
        "operationId": "weaviate.aclEntries.patch",
        "parameters": [
          {
            "type": "string",
            "description": "ID of the device to use.",
            "name": "deviceId",
            "in": "path",
            "required": true
          },
          {
            "type": "string",
            "description": "Unique ACL entry ID.",
            "name": "aclEntryId",
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
          },
          {
            "type": "string",
            "description": "Specifies the language code that should be used for text values in the API response.",
            "name": "hl",
            "in": "query"
          }
        ],
        "responses": {
          "200": {
            "description": "Successful updated.",
            "schema": {
              "$ref": "#/definitions/AclEntry"
            }
          },
          "404": {
            "description": "Successful query result but no resource was found."
          },
          "501": {
            "description": "Not (yet) implemented."
          }
        }
      },
      "parameters": [
        {
          "$ref": "#/parameters/alt"
        },
        {
          "$ref": "#/parameters/fields"
        },
        {
          "$ref": "#/parameters/key"
        },
        {
          "$ref": "#/parameters/oauth_token"
        },
        {
          "$ref": "#/parameters/prettyPrint"
        },
        {
          "$ref": "#/parameters/quotaUser"
        },
        {
          "$ref": "#/parameters/userIp"
        }
      ]
    },
    "/events": {
      "get": {
        "description": "Lists events.",
        "tags": [
          "events"
        ],
        "operationId": "weaviate.events.list",
        "parameters": [
          {
            "type": "array",
            "items": {
              "type": "string"
            },
            "collectionFormat": "multi",
            "description": "Affected command id.",
            "name": "commandId",
            "in": "query"
          },
          {
            "type": "array",
            "items": {
              "type": "string"
            },
            "collectionFormat": "multi",
            "description": "Sending or affected device id.",
            "name": "deviceId",
            "in": "query"
          },
          {
            "type": "string",
            "description": "End of time range in ms since epoch.",
            "name": "endTimeMs",
            "in": "query"
          },
          {
            "type": "string",
            "description": "Specifies the language code that should be used for text values in the API response.",
            "name": "hl",
            "in": "query"
          },
          {
            "type": "integer",
            "name": "maxResults",
            "in": "query"
          },
          {
            "type": "integer",
            "name": "startIndex",
            "in": "query"
          },
          {
            "type": "string",
            "description": "Start of time range in ms since epoch.",
            "name": "startTimeMs",
            "in": "query"
          },
          {
            "type": "string",
            "name": "token",
            "in": "query"
          },
          {
            "enum": [
              "adapterDeactivated",
              "commandCancelled",
              "commandCreated",
              "commandDeleted",
              "commandExpired",
              "commandUpdated",
              "deviceAclUpdated",
              "deviceConnectivityChange",
              "deviceCreated",
              "deviceDeleted",
              "deviceLocationUpdated",
              "deviceTransferred",
              "deviceUpdated",
              "deviceUseTimeUpdated",
              "deviceUserAclCreated",
              "deviceUserAclDeleted",
              "deviceUserAclUpdated",
              "eventsDeleted",
              "eventsRecordingDisabled",
              "eventsRecordingEnabled",
              "locationCreated",
              "locationDeleted",
              "locationMemberAdded",
              "locationMemberRemoved",
              "locationUpdated"
            ],
            "type": "string",
            "description": "Event type.",
            "name": "type",
            "in": "query"
          }
        ],
        "responses": {
          "200": {
            "description": "Successful response.",
            "schema": {
              "$ref": "#/definitions/EventsListResponse"
            }
          },
          "404": {
            "description": "Successful query result but no resource was found."
          },
          "501": {
            "description": "Not (yet) implemented."
          }
        }
      },
      "parameters": [
        {
          "$ref": "#/parameters/alt"
        },
        {
          "$ref": "#/parameters/fields"
        },
        {
          "$ref": "#/parameters/key"
        },
        {
          "$ref": "#/parameters/oauth_token"
        },
        {
          "$ref": "#/parameters/prettyPrint"
        },
        {
          "$ref": "#/parameters/quotaUser"
        },
        {
          "$ref": "#/parameters/userIp"
        }
      ]
    },
    "/events/recordDeviceEvents": {
      "post": {
        "description": "Enables or disables recording of a particular device's events based on a boolean parameter. Enabled by default.",
        "tags": [
          "events"
        ],
        "operationId": "weaviate.events.recordDeviceEvents",
        "parameters": [
          {
            "type": "string",
            "description": "Specifies the language code that should be used for text values in the API response.",
            "name": "hl",
            "in": "query"
          },
          {
            "name": "body",
            "in": "body",
            "required": true,
            "schema": {
              "$ref": "#/definitions/EventsRecordDeviceEventsRequest"
            }
          }
        ],
        "responses": {
          "202": {
            "description": "Successfully received."
          },
          "501": {
            "description": "Not (yet) implemented."
          }
        }
      },
      "parameters": [
        {
          "$ref": "#/parameters/alt"
        },
        {
          "$ref": "#/parameters/fields"
        },
        {
          "$ref": "#/parameters/key"
        },
        {
          "$ref": "#/parameters/oauth_token"
        },
        {
          "$ref": "#/parameters/prettyPrint"
        },
        {
          "$ref": "#/parameters/quotaUser"
        },
        {
          "$ref": "#/parameters/userIp"
        }
      ]
    },
    "/events/{eventId}": {
      "get": {
        "description": "Returns a particular event data.",
        "tags": [
          "events"
        ],
        "operationId": "weaviate.events.get",
        "parameters": [
          {
            "type": "string",
            "description": "Unique ID of the event.",
            "name": "eventId",
            "in": "path",
            "required": true
          },
          {
            "type": "string",
            "description": "Specifies the language code that should be used for text values in the API response.",
            "name": "hl",
            "in": "query"
          }
        ],
        "responses": {
          "200": {
            "description": "Successful response.",
            "schema": {
              "$ref": "#/definitions/Event"
            }
          },
          "404": {
            "description": "Successful query result but no resource was found."
          },
          "501": {
            "description": "Not (yet) implemented."
          }
        }
      },
      "parameters": [
        {
          "$ref": "#/parameters/alt"
        },
        {
          "$ref": "#/parameters/fields"
        },
        {
          "$ref": "#/parameters/key"
        },
        {
          "$ref": "#/parameters/oauth_token"
        },
        {
          "$ref": "#/parameters/prettyPrint"
        },
        {
          "$ref": "#/parameters/quotaUser"
        },
        {
          "$ref": "#/parameters/userIp"
        }
      ]
    },
    "/locations": {
      "get": {
        "description": "Lists all locations.",
        "tags": [
          "locations"
        ],
        "operationId": "weaviate.locations.list",
        "parameters": [
          {
            "type": "array",
            "items": {
              "type": "string"
            },
            "collectionFormat": "multi",
            "description": "Location IDs to include in the result",
            "name": "ids",
            "in": "query",
            "required": true
          },
          {
            "type": "string",
            "description": "Specifies the language code that should be used for text values in the API response.",
            "name": "hl",
            "in": "query"
          },
          {
            "type": "integer",
            "name": "maxResults",
            "in": "query"
          },
          {
            "type": "integer",
            "name": "startIndex",
            "in": "query"
          },
          {
            "type": "string",
            "name": "token",
            "in": "query"
          }
        ],
        "responses": {
          "200": {
            "description": "Successful response.",
            "schema": {
              "$ref": "#/definitions/LocationsListResponse"
            }
          },
          "404": {
            "description": "Successful query result but no resource was found."
          },
          "501": {
            "description": "Not (yet) implemented."
          }
        }
      },
      "post": {
        "description": "Inserts location.",
        "tags": [
          "locations"
        ],
        "operationId": "weaviate.locations.insert",
        "parameters": [
          {
            "name": "body",
            "in": "body",
            "required": true,
            "schema": {
              "$ref": "#/definitions/Location"
            }
          }
        ],
        "responses": {
          "202": {
            "description": "Successfully received.",
            "schema": {
              "$ref": "#/definitions/Location"
            }
          },
          "501": {
            "description": "Not (yet) implemented."
          }
        }
      },
      "parameters": [
        {
          "$ref": "#/parameters/alt"
        },
        {
          "$ref": "#/parameters/fields"
        },
        {
          "$ref": "#/parameters/key"
        },
        {
          "$ref": "#/parameters/oauth_token"
        },
        {
          "$ref": "#/parameters/prettyPrint"
        },
        {
          "$ref": "#/parameters/quotaUser"
        },
        {
          "$ref": "#/parameters/userIp"
        }
      ]
    },
    "/locations/{locationId}": {
      "get": {
        "description": "Get a location.",
        "tags": [
          "locations"
        ],
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
              "$ref": "#/definitions/Location"
            }
          },
          "404": {
            "description": "Successful query result but no resource was found."
          },
          "501": {
            "description": "Not (yet) implemented."
          }
        }
      },
      "put": {
        "description": "Updates an location.",
        "tags": [
          "locations"
        ],
        "operationId": "weaviate.locations.update",
        "parameters": [
          {
            "name": "body",
            "in": "body",
            "required": true,
            "schema": {
              "$ref": "#/definitions/Location"
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
              "$ref": "#/definitions/Location"
            }
          },
          "404": {
            "description": "Successful query result but no resource was found."
          },
          "501": {
            "description": "Not (yet) implemented."
          }
        }
      },
      "delete": {
        "description": "Deletes an location.",
        "tags": [
          "locations"
        ],
        "operationId": "weaviate.locations.delete",
        "parameters": [
          {
            "type": "string",
            "name": "locationId",
            "in": "path",
            "required": true
          }
        ],
        "responses": {
          "204": {
            "description": "Successful deleted."
          },
          "404": {
            "description": "Successful query result but no resource was found."
          },
          "501": {
            "description": "Not (yet) implemented."
          }
        }
      },
      "patch": {
        "description": "Updates an location. This method supports patch semantics.",
        "tags": [
          "locations"
        ],
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
              "$ref": "#/definitions/Location"
            }
          },
          "404": {
            "description": "Successful query result but no resource was found."
          },
          "501": {
            "description": "Not (yet) implemented."
          }
        }
      },
      "parameters": [
        {
          "$ref": "#/parameters/alt"
        },
        {
          "$ref": "#/parameters/fields"
        },
        {
          "$ref": "#/parameters/key"
        },
        {
          "$ref": "#/parameters/oauth_token"
        },
        {
          "$ref": "#/parameters/prettyPrint"
        },
        {
          "$ref": "#/parameters/quotaUser"
        },
        {
          "$ref": "#/parameters/userIp"
        }
      ]
    },
    "/modelManifests": {
      "get": {
        "description": "Lists all model manifests.",
        "tags": [
          "modelManifests"
        ],
        "operationId": "weaviate.modelManifests.list",
        "parameters": [
          {
            "type": "array",
            "items": {
              "type": "string"
            },
            "collectionFormat": "multi",
            "description": "Model manifest IDs to include in the result",
            "name": "ids",
            "in": "query",
            "required": true
          },
          {
            "type": "string",
            "description": "Specifies the language code that should be used for text values in the API response.",
            "name": "hl",
            "in": "query"
          },
          {
            "type": "integer",
            "name": "maxResults",
            "in": "query"
          },
          {
            "type": "integer",
            "name": "startIndex",
            "in": "query"
          },
          {
            "type": "string",
            "name": "token",
            "in": "query"
          }
        ],
        "responses": {
          "200": {
            "description": "Successful response.",
            "schema": {
              "$ref": "#/definitions/ModelManifestsListResponse"
            }
          },
          "404": {
            "description": "Successful query result but no resource was found."
          },
          "501": {
            "description": "Not (yet) implemented."
          }
        }
      },
      "post": {
        "description": "Inserts a model manifest.",
        "tags": [
          "modelManifests"
        ],
        "operationId": "weaviate.modelManifests.create",
        "parameters": [
          {
            "name": "body",
            "in": "body",
            "required": true,
            "schema": {
              "$ref": "#/definitions/ModelManifest"
            }
          }
        ],
        "responses": {
          "202": {
            "description": "Successfully received.",
            "schema": {
              "$ref": "#/definitions/ModelManifest"
            }
          },
          "501": {
            "description": "Not (yet) implemented."
          }
        }
      },
      "parameters": [
        {
          "$ref": "#/parameters/alt"
        },
        {
          "$ref": "#/parameters/fields"
        },
        {
          "$ref": "#/parameters/key"
        },
        {
          "$ref": "#/parameters/oauth_token"
        },
        {
          "$ref": "#/parameters/prettyPrint"
        },
        {
          "$ref": "#/parameters/quotaUser"
        },
        {
          "$ref": "#/parameters/userIp"
        }
      ]
    },
    "/modelManifests/validateCommandDefs": {
      "post": {
        "description": "Validates given command definitions and returns errors.",
        "tags": [
          "modelManifests"
        ],
        "operationId": "weaviate.modelManifests.validateCommandDefs",
        "parameters": [
          {
            "type": "string",
            "description": "Specifies the language code that should be used for text values in the API response.",
            "name": "hl",
            "in": "query"
          },
          {
            "name": "body",
            "in": "body",
            "required": true,
            "schema": {
              "$ref": "#/definitions/ModelManifestsValidateCommandDefsRequest"
            }
          }
        ],
        "responses": {
          "201": {
            "description": "Successful created.",
            "schema": {
              "$ref": "#/definitions/ModelManifestsValidateCommandDefsResponse"
            }
          },
          "501": {
            "description": "Not (yet) implemented."
          }
        }
      },
      "parameters": [
        {
          "$ref": "#/parameters/alt"
        },
        {
          "$ref": "#/parameters/fields"
        },
        {
          "$ref": "#/parameters/key"
        },
        {
          "$ref": "#/parameters/oauth_token"
        },
        {
          "$ref": "#/parameters/prettyPrint"
        },
        {
          "$ref": "#/parameters/quotaUser"
        },
        {
          "$ref": "#/parameters/userIp"
        }
      ]
    },
    "/modelManifests/validateComponents": {
      "post": {
        "description": "Validates given components definitions and returns errors.",
        "tags": [
          "modelManifests"
        ],
        "operationId": "weaviate.modelManifests.validateComponents",
        "parameters": [
          {
            "type": "string",
            "description": "Specifies the language code that should be used for text values in the API response.",
            "name": "hl",
            "in": "query"
          },
          {
            "name": "body",
            "in": "body",
            "required": true,
            "schema": {
              "$ref": "#/definitions/ModelManifestsValidateComponentsRequest"
            }
          }
        ],
        "responses": {
          "201": {
            "description": "Successful created.",
            "schema": {
              "$ref": "#/definitions/ModelManifestsValidateComponentsResponse"
            }
          },
          "501": {
            "description": "Not (yet) implemented."
          }
        }
      },
      "parameters": [
        {
          "$ref": "#/parameters/alt"
        },
        {
          "$ref": "#/parameters/fields"
        },
        {
          "$ref": "#/parameters/key"
        },
        {
          "$ref": "#/parameters/oauth_token"
        },
        {
          "$ref": "#/parameters/prettyPrint"
        },
        {
          "$ref": "#/parameters/quotaUser"
        },
        {
          "$ref": "#/parameters/userIp"
        }
      ]
    },
    "/modelManifests/validateDeviceState": {
      "post": {
        "description": "Validates given device state object and returns errors.",
        "tags": [
          "modelManifests"
        ],
        "operationId": "weaviate.modelManifests.validateDeviceState",
        "parameters": [
          {
            "type": "string",
            "description": "Specifies the language code that should be used for text values in the API response.",
            "name": "hl",
            "in": "query"
          },
          {
            "name": "body",
            "in": "body",
            "required": true,
            "schema": {
              "$ref": "#/definitions/ModelManifestsValidateDeviceStateRequest"
            }
          }
        ],
        "responses": {
          "201": {
            "description": "Successful created.",
            "schema": {
              "$ref": "#/definitions/ModelManifestsValidateDeviceStateResponse"
            }
          },
          "501": {
            "description": "Not (yet) implemented."
          }
        }
      },
      "parameters": [
        {
          "$ref": "#/parameters/alt"
        },
        {
          "$ref": "#/parameters/fields"
        },
        {
          "$ref": "#/parameters/key"
        },
        {
          "$ref": "#/parameters/oauth_token"
        },
        {
          "$ref": "#/parameters/prettyPrint"
        },
        {
          "$ref": "#/parameters/quotaUser"
        },
        {
          "$ref": "#/parameters/userIp"
        }
      ]
    },
    "/modelManifests/{modelManifestId}": {
      "get": {
        "description": "Returns a particular model manifest.",
        "tags": [
          "modelManifests"
        ],
        "operationId": "weaviate.modelManifests.get",
        "parameters": [
          {
            "type": "string",
            "description": "Unique ID of the model manifest.",
            "name": "modelManifestId",
            "in": "path",
            "required": true
          },
          {
            "type": "string",
            "description": "Specifies the language code that should be used for text values in the API response.",
            "name": "hl",
            "in": "query"
          }
        ],
        "responses": {
          "200": {
            "description": "Successful response.",
            "schema": {
              "$ref": "#/definitions/ModelManifest"
            }
          },
          "404": {
            "description": "Successful query result but no resource was found."
          },
          "501": {
            "description": "Not (yet) implemented."
          }
        }
      },
      "put": {
        "description": "Updates a particular model manifest.",
        "tags": [
          "modelManifests"
        ],
        "operationId": "weaviate.modelManifests.update",
        "parameters": [
          {
            "type": "string",
            "description": "Unique ID of the model manifest.",
            "name": "modelManifestId",
            "in": "path",
            "required": true
          },
          {
            "name": "body",
            "in": "body",
            "required": true,
            "schema": {
              "$ref": "#/definitions/ModelManifest"
            }
          }
        ],
        "responses": {
          "200": {
            "description": "Successful updated.",
            "schema": {
              "$ref": "#/definitions/ModelManifest"
            }
          },
          "404": {
            "description": "Successful query result but no resource was found."
          },
          "501": {
            "description": "Not (yet) implemented."
          }
        }
      },
      "delete": {
        "description": "Deletes a particular model manifest.",
        "tags": [
          "modelManifests"
        ],
        "operationId": "weaviate.modelManifests.delete",
        "parameters": [
          {
            "type": "string",
            "description": "Unique ID of the model manifest.",
            "name": "modelManifestId",
            "in": "path",
            "required": true
          }
        ],
        "responses": {
          "204": {
            "description": "Successful deleted."
          },
          "404": {
            "description": "Successful query result but no resource was found."
          },
          "501": {
            "description": "Not (yet) implemented."
          }
        }
      },
      "patch": {
        "description": "Updates a particular model manifest.",
        "tags": [
          "modelManifests"
        ],
        "operationId": "weaviate.modelManifests.patch",
        "parameters": [
          {
            "type": "string",
            "description": "Unique ID of the model manifest.",
            "name": "modelManifestId",
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
              "$ref": "#/definitions/ModelManifest"
            }
          },
          "404": {
            "description": "Successful query result but no resource was found."
          },
          "501": {
            "description": "Not (yet) implemented."
          }
        }
      },
      "parameters": [
        {
          "$ref": "#/parameters/alt"
        },
        {
          "$ref": "#/parameters/fields"
        },
        {
          "$ref": "#/parameters/key"
        },
        {
          "$ref": "#/parameters/oauth_token"
        },
        {
          "$ref": "#/parameters/prettyPrint"
        },
        {
          "$ref": "#/parameters/quotaUser"
        },
        {
          "$ref": "#/parameters/userIp"
        }
      ]
    }
  },
  "definitions": {
    "AclEntriesListResponse": {
      "description": "List of Access control list entries.",
      "type": "object",
      "properties": {
        "aclEntries": {
          "description": "The actual list of ACL entries.",
          "type": "array",
          "items": {
            "$ref": "#/definitions/AclEntry"
          }
        },
        "kind": {
          "description": "Identifies what kind of resource this is. Value: the fixed string \"weave#aclEntriesListResponse\".",
          "type": "string",
          "default": "weave#aclEntriesListResponse"
        },
        "nextPageToken": {
          "description": "Token corresponding to the next page of ACL entries.",
          "type": "string"
        },
        "totalResults": {
          "description": "The total number of ACL entries for the query. The number of items in a response may be smaller due to paging.",
          "type": "integer",
          "format": "int32"
        }
      }
    },
    "AclEntry": {
      "type": "object",
      "properties": {
        "cloudAccessRevoked": {
          "description": "Indicates whether the AclEntry has been revoked from the cloud and the user has no cloud access, but they still might have local auth tokens that are valid and can access the device and execute commands locally. See localAccessInfo for local auth details.",
          "type": "boolean"
        },
        "creatorEmail": {
          "description": "User who created this entry. At the moment it is populated only when pending == true.",
          "type": "string"
        },
        "delegator": {
          "description": "User on behalf of whom the access is granted to the application.",
          "type": "string"
        },
        "id": {
          "description": "Unique ACL entry ID.",
          "type": "string"
        },
        "key": {
          "description": "Public access key value. Set only when scopeType is PUBLIC.",
          "type": "string",
          "format": "int64"
        },
        "kind": {
          "description": "Identifies what kind of resource this is. Value: the fixed string \"weave#aclEntry\".",
          "type": "string",
          "default": "weave#aclEntry"
        },
        "localAccessInfo": {
          "$ref": "#/definitions/LocalAccessInfo"
        },
        "pending": {
          "description": "Whether this ACL entry is pending for user reply to accept/reject it.",
          "type": "boolean"
        },
        "privileges": {
          "description": "Set of access privileges granted for this scope.\n\nValid values are:  \n- \"modifyAcl\" \n- \"viewAllEvents\"",
          "type": "array",
          "items": {
            "type": "string",
            "enum": [
              "modifyAcl",
              "viewAllEvents"
            ]
          }
        },
        "revocationTimeMs": {
          "description": "Time in milliseconds since Unix Epoch indicating when the AclEntry was revoked.",
          "type": "string",
          "format": "int64"
        },
        "role": {
          "description": "Access role granted to this scope.",
          "type": "string",
          "enum": [
            "manager",
            "owner",
            "robot",
            "user",
            "viewer"
          ]
        },
        "scopeId": {
          "description": "Email address if scope type is user or group, domain name if scope type is a domain.",
          "type": "string"
        },
        "scopeMembership": {
          "description": "Type of membership the user has in the scope.",
          "type": "string",
          "enum": [
            "delegator",
            "manager",
            "member",
            "none"
          ]
        },
        "scopeName": {
          "description": "Displayable scope name.",
          "type": "string"
        },
        "scopePhotoUrl": {
          "description": "URL of this scope displayable photo.",
          "type": "string"
        },
        "scopeType": {
          "description": "Type of the access scope.",
          "type": "string",
          "enum": [
            "application",
            "domain",
            "group",
            "public",
            "user"
          ]
        }
      }
    },
    "Adapter": {
      "type": "object",
      "properties": {
        "activateUrl": {
          "description": "URL to adapter web flow to activate the adapter. Deprecated, use the activationUrl returned in the response of the Adapters.activate API.",
          "type": "string"
        },
        "activated": {
          "description": "Whether this adapter has been activated for the current user.",
          "type": "boolean"
        },
        "deactivateUrl": {
          "description": "URL to adapter web flow to disconnect the adapter. Deprecated, the adapter will be notified via pubsub.",
          "type": "string"
        },
        "displayName": {
          "description": "Display name of the adapter.",
          "type": "string"
        },
        "iconUrl": {
          "description": "URL to an icon that represents the adapter.",
          "type": "string"
        },
        "id": {
          "description": "ID of the adapter.",
          "type": "string"
        },
        "manageUrl": {
          "description": "URL to adapter web flow to connect new devices. Only used for adapters that cannot automatically detect new devices. This field is returned only if the user has already activated the adapter.",
          "type": "string"
        }
      }
    },
    "AdaptersListResponse": {
      "type": "object",
      "properties": {
        "adapters": {
          "description": "The list of adapters.",
          "type": "array",
          "items": {
            "$ref": "#/definitions/Adapter"
          }
        },
        "kind": {
          "description": "Identifies what kind of resource this is. Value: the fixed string \"weave#adaptersListResponse\".",
          "type": "string",
          "default": "weave#adaptersListResponse"
        }
      }
    },
    "Application": {
      "description": "Contains information about a recommended application for a device model.",
      "type": "object",
      "properties": {
        "description": {
          "description": "User readable application description.",
          "type": "string"
        },
        "iconUrl": {
          "description": "Application icon URL.",
          "type": "string"
        },
        "id": {
          "description": "Unique application ID.",
          "type": "string"
        },
        "name": {
          "description": "User readable application name.",
          "type": "string"
        },
        "price": {
          "description": "Price of the application.",
          "type": "number",
          "format": "double"
        },
        "publisherName": {
          "description": "User readable publisher name.",
          "type": "string"
        },
        "type": {
          "description": "Application type.",
          "type": "string",
          "enum": [
            "android",
            "chrome",
            "ios",
            "web"
          ]
        },
        "url": {
          "description": "Application install URL.",
          "type": "string"
        }
      }
    },
    "AssociatedLabel": {
      "type": "object",
      "properties": {
        "id": {
          "type": "string"
        },
        "name": {
          "type": "string"
        }
      }
    },
    "Command": {
      "type": "object",
      "properties": {
        "blobParameters": {
          "$ref": "#/definitions/JsonObject"
        },
        "blobResults": {
          "$ref": "#/definitions/JsonObject"
        },
        "component": {
          "description": "Component name paths separated by '/'.",
          "type": "string"
        },
        "creationTimeMs": {
          "description": "Timestamp since epoch of a creation of a command.",
          "type": "string",
          "format": "int64"
        },
        "creatorEmail": {
          "description": "User that created the command (not applicable if the user is deleted).",
          "type": "string"
        },
        "deviceId": {
          "description": "Device ID that this command belongs to.",
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
          "type": "string",
          "format": "int64"
        },
        "expirationTimeoutMs": {
          "description": "Expiration timeout for the command since its creation, 10 seconds min, 30 days max.",
          "type": "string",
          "format": "int64"
        },
        "id": {
          "description": "Unique command ID.",
          "type": "string"
        },
        "kind": {
          "description": "Identifies what kind of resource this is. Value: the fixed string \"weave#command\".",
          "type": "string",
          "default": "weave#command"
        },
        "lastUpdateTimeMs": {
          "description": "Timestamp since epoch of last update made to the command.",
          "type": "string",
          "format": "int64"
        },
        "name": {
          "description": "Full command name, including trait.",
          "type": "string"
        },
        "parameters": {
          "$ref": "#/definitions/JsonObject"
        },
        "progress": {
          "$ref": "#/definitions/JsonObject"
        },
        "results": {
          "$ref": "#/definitions/JsonObject"
        },
        "state": {
          "description": "Current command state.",
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
        "userAction": {
          "description": "Pending command state that is not acknowledged by the device yet.",
          "type": "string"
        }
      }
    },
    "CommandsListResponse": {
      "description": "List of commands.",
      "type": "object",
      "properties": {
        "commands": {
          "description": "The actual list of commands.",
          "type": "array",
          "items": {
            "$ref": "#/definitions/Command"
          }
        },
        "kind": {
          "description": "Identifies what kind of resource this is. Value: the fixed string \"weave#commandsListResponse\".",
          "type": "string",
          "default": "weave#commandsListResponse"
        },
        "nextPageToken": {
          "description": "Token for the next page of commands.",
          "type": "string"
        },
        "totalResults": {
          "description": "The total number of commands for the query. The number of items in a response may be smaller due to paging.",
          "type": "integer",
          "format": "int32"
        }
      }
    },
    "CommandsQueueResponse": {
      "type": "object",
      "properties": {
        "commands": {
          "description": "Commands to be executed.",
          "type": "array",
          "items": {
            "$ref": "#/definitions/Command"
          }
        }
      }
    },
    "Device": {
      "type": "object",
      "properties": {
        "adapterId": {
          "description": "ID of the adapter that created this device.",
          "type": "string"
        },
        "certFingerprint": {
          "description": "Deprecated, do not use. The HTTPS certificate fingerprint used to secure communication with device..",
          "type": "string"
        },
        "channel": {
          "description": "Device notification channel description.",
          "type": "object",
          "properties": {
            "connectionStatusHint": {
              "description": "Connection status hint, set by parent device.",
              "type": "string",
              "enum": [
                "offline",
                "online",
                "unknown"
              ]
            },
            "gcmRegistrationId": {
              "description": "GCM registration ID. Required if device supports GCM delivery channel.",
              "type": "string"
            },
            "gcmSenderId": {
              "description": "GCM sender ID. For Chrome apps must be the same as sender ID during registration, usually API project ID.",
              "type": "string"
            },
            "parentId": {
              "description": "Parent device ID (aggregator) if it exists.",
              "type": "string"
            },
            "pubsub": {
              "description": "Pubsub channel details.",
              "type": "object",
              "properties": {
                "connectionStatusHint": {
                  "description": "Device's connection status, as set by the pubsub subscriber.",
                  "type": "string",
                  "enum": [
                    "offline",
                    "online",
                    "unknown"
                  ]
                },
                "topic": {
                  "description": "Pubsub topic to publish to with device notifications.",
                  "type": "string"
                }
              }
            },
            "supportedType": {
              "description": "Channel type supported by device. Allowed types are: \"gcm\", \"xmpp\", \"pubsub\", and \"parent\".",
              "type": "string"
            }
          }
        },
        "commandDefs": {
          "description": "Deprecated, use \"traits\" instead. Description of commands supported by the device. This field is writable only by devices.",
          "type": "object",
          "additionalProperties": {
            "$ref": "#/definitions/PackageDef"
          }
        },
        "components": {
          "$ref": "#/definitions/JsonObject"
        },
        "connectionStatus": {
          "description": "Device connection status.",
          "type": "string"
        },
        "creationTimeMs": {
          "description": "Timestamp of creation of this device in milliseconds since epoch UTC.",
          "type": "string",
          "format": "int64"
        },
        "description": {
          "description": "User readable description of this device.",
          "type": "string"
        },
        "deviceKind": {
          "description": "Device kind. Deprecated, provide \"modelManifestId\" instead. See list of device kinds values.",
          "type": "string"
        },
        "deviceLocalId": {
          "description": "Deprecated, do not use. The ID of the device for use on the local network.",
          "type": "string"
        },
        "id": {
          "description": "Unique device ID.",
          "type": "string"
        },
        "invitations": {
          "description": "List of pending invitations for the currently logged-in user.",
          "type": "array",
          "items": {
            "$ref": "#/definitions/Invitation"
          }
        },
        "isEventRecordingDisabled": {
          "description": "Indicates whether event recording is enabled or disabled for this device.",
          "type": "boolean"
        },
        "kind": {
          "description": "Identifies what kind of resource this is. Value: the fixed string \"weave#device\".",
          "type": "string",
          "default": "weave#device"
        },
        "labels": {
          "description": "Any labels attached to the device. Use the addLabel and removeLabel APIs to modify this list.",
          "type": "array",
          "items": {
            "$ref": "#/definitions/AssociatedLabel"
          }
        },
        "lastSeenTimeMs": {
          "description": "Timestamp of the last request from this device in milliseconds since epoch UTC. Supported only for devices with XMPP channel type.",
          "type": "string",
          "format": "int64"
        },
        "lastUpdateTimeMs": {
          "description": "Timestamp of the last device update in milliseconds since epoch UTC.",
          "type": "string",
          "format": "int64"
        },
        "lastUseTimeMs": {
          "description": "Timestamp of the last device usage in milliseconds since epoch UTC.",
          "type": "string",
          "format": "int64"
        },
        "locationId": {
          "description": "ID of the location of this device.",
          "type": "string",
          "format": "int64"
        },
        "modelManifest": {
          "description": "Device model information provided by the model manifest of this device.",
          "type": "object",
          "properties": {
            "modelName": {
              "description": "Device model name.",
              "type": "string"
            },
            "oemName": {
              "description": "Name of device model manufacturer.",
              "type": "string"
            }
          }
        },
        "modelManifestId": {
          "description": "Model manifest ID of this device.",
          "type": "string"
        },
        "name": {
          "description": "Name of this device provided by the manufacturer.",
          "type": "string"
        },
        "nicknames": {
          "description": "Nicknames of the device. Use the addNickname and removeNickname APIs to modify this list.",
          "type": "array",
          "items": {
            "type": "string"
          }
        },
        "owner": {
          "description": "E-mail address of the device owner.",
          "type": "string"
        },
        "personalizedInfo": {
          "description": "Personalized device information for currently logged-in user.",
          "type": "object",
          "properties": {
            "lastUseTimeMs": {
              "description": "Timestamp of the last device usage by the user in milliseconds since epoch UTC.",
              "type": "string",
              "format": "int64"
            },
            "location": {
              "description": "Personalized device location.",
              "type": "string"
            },
            "maxRole": {
              "description": "The maximum role on the device.",
              "type": "string"
            },
            "name": {
              "description": "Personalized device display name.",
              "type": "string"
            }
          }
        },
        "serialNumber": {
          "description": "Serial number of a device provided by its manufacturer.",
          "type": "string"
        },
        "state": {
          "$ref": "#/definitions/JsonObject"
        },
        "stateDefs": {
          "description": "Deprecated, do not use. Description of the device state. This field is writable only by devices.",
          "type": "object",
          "additionalProperties": {
            "$ref": "#/definitions/StateDef"
          }
        },
        "tags": {
          "description": "Custom free-form manufacturer tags.",
          "type": "array",
          "items": {
            "type": "string"
          }
        },
        "traits": {
          "$ref": "#/definitions/JsonObject"
        },
        "uiDeviceKind": {
          "description": "Device kind from the model manifest used in UI applications. See list of device kinds values.",
          "type": "string"
        }
      }
    },
    "DevicesListResponse": {
      "description": "List of devices.",
      "type": "object",
      "properties": {
        "devices": {
          "description": "The actual list of devices.",
          "type": "array",
          "items": {
            "$ref": "#/definitions/Device"
          }
        },
        "kind": {
          "description": "Identifies what kind of resource this is. Value: the fixed string \"weave#devicesListResponse\".",
          "type": "string",
          "default": "weave#devicesListResponse"
        },
        "nextPageToken": {
          "description": "Token corresponding to the next page of devices.",
          "type": "string"
        },
        "totalResults": {
          "description": "The total number of devices for the query. The number of items in a response may be smaller due to paging.",
          "type": "integer",
          "format": "int32"
        }
      }
    },
    "Event": {
      "type": "object",
      "properties": {
        "commandPatch": {
          "description": "Command-related changes (if applicable).",
          "type": "object",
          "properties": {
            "commandId": {
              "description": "ID of the affected command.",
              "type": "string"
            },
            "state": {
              "description": "New command state.",
              "type": "string"
            }
          }
        },
        "connectionStatus": {
          "description": "New device connection state (if connectivity change event).",
          "type": "string"
        },
        "deviceId": {
          "description": "The device that was affected by this event.",
          "type": "string"
        },
        "id": {
          "description": "ID of the event.",
          "type": "string"
        },
        "kind": {
          "description": "Identifies what kind of resource this is. Value: the fixed string \"weave#event\".",
          "type": "string",
          "default": "weave#event"
        },
        "statePatch": {
          "$ref": "#/definitions/JsonObject"
        },
        "timeMs": {
          "description": "Time the event was generated in milliseconds since epoch UTC.",
          "type": "string",
          "format": "int64"
        },
        "type": {
          "description": "Type of the event.",
          "type": "string",
          "enum": [
            "adapterDeactivated",
            "commandCancelled",
            "commandCreated",
            "commandDeleted",
            "commandExpired",
            "commandUpdated",
            "deviceAclUpdated",
            "deviceConnectivityChange",
            "deviceCreated",
            "deviceDeleted",
            "deviceLocationUpdated",
            "deviceTransferred",
            "deviceUpdated",
            "deviceUseTimeUpdated",
            "deviceUserAclCreated",
            "deviceUserAclDeleted",
            "deviceUserAclUpdated",
            "eventsDeleted",
            "eventsRecordingDisabled",
            "eventsRecordingEnabled",
            "locationCreated",
            "locationDeleted",
            "locationMemberAdded",
            "locationMemberRemoved",
            "locationUpdated"
          ]
        },
        "userEmail": {
          "description": "User that caused the event (if applicable).",
          "type": "string"
        }
      }
    },
    "EventsListResponse": {
      "description": "List of events.",
      "type": "object",
      "properties": {
        "events": {
          "description": "The actual list of events in reverse chronological order.",
          "type": "array",
          "items": {
            "$ref": "#/definitions/Event"
          }
        },
        "kind": {
          "description": "Identifies what kind of resource this is. Value: the fixed string \"weave#eventsListResponse\".",
          "type": "string",
          "default": "weave#eventsListResponse"
        },
        "nextPageToken": {
          "description": "Token for the next page of events.",
          "type": "string"
        },
        "totalResults": {
          "description": "The total number of events for the query. The number of items in a response may be smaller due to paging.",
          "type": "integer",
          "format": "int32"
        }
      }
    },
    "EventsRecordDeviceEventsRequest": {
      "type": "object",
      "properties": {
        "deviceId": {
          "description": "Device ID.",
          "type": "string"
        },
        "recordDeviceEvents": {
          "description": "Flag to indicate whether recording should be enabled or disabled.",
          "type": "boolean"
        }
      }
    },
    "Invitation": {
      "type": "object",
      "properties": {
        "aclEntry": {
          "$ref": "#/definitions/AclEntry"
        },
        "creatorEmail": {
          "description": "Email of a user who created this invitation.",
          "type": "string"
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
    "LocalAccessEntry": {
      "type": "object",
      "properties": {
        "isApp": {
          "description": "Whether this belongs to a delegated app or user.",
          "type": "boolean"
        },
        "localAccessRole": {
          "description": "Access role of the user.",
          "type": "string",
          "enum": [
            "manager",
            "owner",
            "robot",
            "user",
            "viewer"
          ]
        },
        "projectId": {
          "description": "Project id of the app that this access info is associated with.",
          "type": "string",
          "format": "int64"
        }
      }
    },
    "LocalAccessInfo": {
      "type": "object",
      "properties": {
        "localAccessEntry": {
          "$ref": "#/definitions/LocalAccessEntry"
        },
        "localAuthTokenMintTimeMs": {
          "description": "Time in milliseconds since unix epoch of when the local auth token was minted.",
          "type": "string",
          "format": "int64"
        },
        "localAuthTokenTimeLeftMs": {
          "description": "Relative time left of token after API call.",
          "type": "string",
          "format": "int64"
        },
        "localAuthTokenTtlTimeMs": {
          "description": "Time in milliseconds of hold long the token is valid after minting.",
          "type": "string",
          "format": "int64"
        }
      }
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
        "id": {
          "description": "ID of the location.",
          "type": "string"
        },
        "place_id": {
          "description": "The ID of the place corresponding the location.",
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
        "locations": {
          "description": "The list of locations.",
          "type": "array",
          "items": {
            "$ref": "#/definitions/Location"
          }
        }
      }
    },
    "ModelManifest": {
      "description": "Model manifest info.",
      "type": "object",
      "properties": {
        "allowedChildModelManifestIds": {
          "description": "For gateways, a list of device ids that are allowed to connect to it.",
          "type": "array",
          "items": {
            "type": "string"
          }
        },
        "applications": {
          "description": "List of applications recommended to use with a device model.",
          "type": "array",
          "items": {
            "$ref": "#/definitions/Application"
          }
        },
        "confirmationImageUrl": {
          "description": "URL of image showing a confirmation button.",
          "type": "string"
        },
        "deviceImageUrl": {
          "description": "URL of device image.",
          "type": "string"
        },
        "deviceKind": {
          "description": "Device kind, see \"deviceKind\" field of the Device resource. See list of device kinds values.",
          "type": "string"
        },
        "id": {
          "description": "Unique model manifest ID.",
          "type": "string"
        },
        "kind": {
          "description": "Identifies what kind of resource this is. Value: the fixed string \"weave#modelManifest\".",
          "type": "string",
          "default": "weave#modelManifest"
        },
        "modelDescription": {
          "description": "User readable device model description.",
          "type": "string"
        },
        "modelName": {
          "description": "User readable device model name.",
          "type": "string"
        },
        "oemName": {
          "description": "User readable name of device model manufacturer.",
          "type": "string"
        },
        "supportPageUrl": {
          "description": "URL of device support page.",
          "type": "string"
        }
      }
    },
    "ModelManifestsListResponse": {
      "description": "List of model manifests.",
      "type": "object",
      "properties": {
        "kind": {
          "description": "Identifies what kind of resource this is. Value: the fixed string \"weave#modelManifestsListResponse\".",
          "type": "string",
          "default": "weave#modelManifestsListResponse"
        },
        "modelManifests": {
          "description": "The actual list of model manifests.",
          "type": "array",
          "items": {
            "$ref": "#/definitions/ModelManifest"
          }
        },
        "nextPageToken": {
          "description": "Token corresponding to the next page of model manifests.",
          "type": "string"
        },
        "totalResults": {
          "description": "The total number of model manifests for the query. The number of items in a response may be smaller due to paging.",
          "type": "integer",
          "format": "int32"
        }
      }
    },
    "ModelManifestsValidateCommandDefsRequest": {
      "type": "object",
      "properties": {
        "commandDefs": {
          "description": "Description of commands.",
          "type": "object",
          "additionalProperties": {
            "$ref": "#/definitions/PackageDef"
          }
        }
      }
    },
    "ModelManifestsValidateCommandDefsResponse": {
      "type": "object",
      "properties": {
        "validationErrors": {
          "description": "Validation errors in command definitions.",
          "type": "array",
          "items": {
            "type": "string"
          }
        }
      }
    },
    "ModelManifestsValidateComponentsRequest": {
      "type": "object",
      "properties": {
        "components": {
          "type": "string"
        },
        "traits": {
          "type": "string"
        }
      }
    },
    "ModelManifestsValidateComponentsResponse": {
      "type": "object",
      "properties": {
        "validationErrors": {
          "description": "Validation errors in component definitions.",
          "type": "array",
          "items": {
            "type": "string"
          }
        }
      }
    },
    "ModelManifestsValidateDeviceStateRequest": {
      "type": "object",
      "properties": {
        "state": {
          "$ref": "#/definitions/JsonObject"
        }
      }
    },
    "ModelManifestsValidateDeviceStateResponse": {
      "type": "object",
      "properties": {
        "validationErrors": {
          "description": "Validation errors in device state.",
          "type": "array",
          "items": {
            "type": "string"
          }
        }
      }
    },
    "PackageDef": {
      "type": "object",
      "additionalProperties": {
        "type": "object",
        "properties": {
          "displayName": {
            "description": "Display name of the command.",
            "type": "string"
          },
          "kind": {
            "description": "Identifies what kind of resource this is. Value: the fixed string \"weave#commandDef\".",
            "type": "string",
            "default": "weave#commandDef"
          },
          "minimalRole": {
            "description": "Minimal role required to execute command.",
            "type": "string",
            "enum": [
              "manager",
              "owner",
              "user",
              "viewer"
            ]
          },
          "parameters": {
            "description": "Parameters of the command.",
            "type": "object",
            "additionalProperties": {
              "$ref": "#/definitions/JsonObject"
            }
          }
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
    "StateDef": {
      "type": "object",
      "additionalProperties": {
        "type": "object",
        "properties": {
          "kind": {
            "description": "Identifies what kind of resource this is. Value: the fixed string \"weave#stateDef\".",
            "type": "string",
            "default": "weave#stateDef"
          },
          "minimalRole": {
            "description": "Minimal role required to view state.",
            "type": "string",
            "enum": [
              "manager",
              "owner",
              "user",
              "viewer"
            ]
          },
          "name": {
            "description": "Name of the state field.",
            "type": "string"
          }
        }
      }
    }
  },
  "parameters": {
    "alt": {
      "enum": [
        "json"
      ],
      "type": "string",
      "default": "json",
      "description": "Data format for the response.",
      "name": "alt",
      "in": "query"
    },
    "fields": {
      "type": "string",
      "description": "Selector specifying which fields to include in a partial response.",
      "name": "fields",
      "in": "query"
    },
    "key": {
      "type": "string",
      "description": "API key. Your API key identifies your project and provides you with API access, quota, and reports. Required unless you provide an OAuth 2.0 token.",
      "name": "key",
      "in": "query"
    },
    "oauth_token": {
      "type": "string",
      "description": "OAuth 2.0 token for the current user.",
      "name": "oauth_token",
      "in": "query"
    },
    "prettyPrint": {
      "type": "boolean",
      "default": true,
      "description": "Returns response with indentations and line breaks.",
      "name": "prettyPrint",
      "in": "query"
    },
    "quotaUser": {
      "type": "string",
      "description": "Available to use for quota purposes for server-side applications. Can be any arbitrary string assigned to a user, but should not exceed 40 characters. Overrides userIp if both are provided.",
      "name": "quotaUser",
      "in": "query"
    },
    "userIp": {
      "type": "string",
      "description": "IP address of the site where the request originates. Use this if you want to enforce per-user limits.",
      "name": "userIp",
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
  "tags": [
    {
      "name": "adapters"
    },
    {
      "name": "commands"
    },
    {
      "name": "devices"
    },
    {
      "name": "events"
    },
    {
      "name": "locations"
    },
    {
      "name": "modelManifests"
    }
  ],
  "externalDocs": {
    "url": "https://github.com/weaviate/weaviate"
  }
}`))
}
