# Module Parameters Export Tool

This tool exports all module configuration parameters to JSON format for SDK developers.

## Overview

The tool collects `Parameters` maps from all refactored vectorizer modules and exports them as structured JSON, making it easy for SDK developers to:
- Generate client code automatically
- Validate user configuration
- Provide autocomplete/IntelliSense
- Generate documentation

## Usage

### Generate JSON output

```bash
# From weaviate root directory
go run tools/export-parameters/main.go > docs/module-parameters.json
```

### In CI/CD

Add to your GitHub Actions workflow:

```yaml
- name: Export module parameters
  run: |
    go run tools/export-parameters/main.go > docs/module-parameters.json
    git add docs/module-parameters.json
```

## Output Format

```json
{
  "version": "1.0.0",
  "generatedBy": "tools/export-parameters",
  "modules": [
    {
      "name": "text2vec-cohere",
      "description": "Cohere vectorizer module",
      "parameters": {
        "Model": {
          "jsonKey": "model",
          "alternateKeys": [],
          "defaultValue": "embed-multilingual-v3.0",
          "description": "Cohere model name",
          "required": false,
          "allowedValues": null
        },
        "Truncate": {
          "jsonKey": "truncate",
          "defaultValue": "END",
          "description": "Truncation strategy",
          "required": false,
          "allowedValues": ["NONE", "START", "END", "LEFT", "RIGHT"]
        }
      }
    }
  ]
}
```

## Fields Explained

| Field | Description |
|-------|-------------|
| `jsonKey` | The actual JSON key used in the API |
| `alternateKeys` | Deprecated keys for backwards compatibility |
| `defaultValue` | Default value if not specified |
| `description` | Human-readable description |
| `required` | Whether the parameter is required |
| `allowedValues` | Valid enum values (if applicable) |

## Adding New Modules

When you refactor a new module to use `ParameterDef`, add it to `main.go`:

```go
modules := []ModuleParameters{
    {
        Name:        "text2vec-cohere",
        Description: "Cohere vectorizer module",
        Parameters:  convertParameters(cohere.Parameters),
    },
    {
        Name:        "text2vec-newmodule",  // Add here
        Description: "New module description",
        Parameters:  convertParameters(newmodule.Parameters),
    },
}
```

## For SDK Developers

### Python Example

```python
import json
import requests

# Fetch parameters
params = requests.get("https://raw.githubusercontent.com/weaviate/weaviate/main/docs/module-parameters.json").json()

# Generate client code
for module in params["modules"]:
    print(f"class {module['name'].replace('-', '_').title()}Config:")
    for param_name, param in module["parameters"].items():
        default = param.get("defaultValue", "None")
        print(f"    {param['jsonKey']}: Optional[str] = {default}")
```

### TypeScript Example

```typescript
import params from './module-parameters.json';

// Generate types
params.modules.forEach(module => {
  console.log(`interface ${module.name}Config {`);
  Object.entries(module.parameters).forEach(([key, param]) => {
    const optional = param.required ? '' : '?';
    console.log(`  ${param.jsonKey}${optional}: string;`);
  });
  console.log('}');
});
```

## Versioning

The output includes a version field for tracking schema changes:
- `1.0.0` - Initial format with ParameterDef structure

## Future Enhancements

- [ ] Add parameter type information (string, int64, etc.)
- [ ] Include validation rules
- [ ] Export as OpenAPI/Swagger spec
- [ ] Add runtime API endpoint
