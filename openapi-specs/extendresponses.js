/**
 * A simple script to add a new response to every single API path. This was
 * built for the purpose of adding 500 Internal Server Error to everything, but
 * could potentially also be used for other purposes in the future.
 */

const fs = require('fs')

const file = fs.readFileSync('./schema.json', 'utf-8')
const parsed = JSON.parse(file)

for (const [pathKey, pathValue] of Object.entries(parsed.paths)) {
  for (const [path, value] of Object.entries(pathValue)) {
    if (!value.responses) {
      continue
    }

    value.responses['500'] = {
      description: "An error has occurred while trying to fulfill the request. Most likely the ErrorResponse will contain more information about the error.",
      schema: {
        "$ref": "#/definitions/ErrorResponse"
      }
    }
  }
}
fs.writeFileSync('./schema.json', JSON.stringify(parsed, null, 2))
