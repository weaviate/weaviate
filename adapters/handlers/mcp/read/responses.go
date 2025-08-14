package read

import "github.com/weaviate/weaviate/entities/models"

type GetTenantsResp struct {
	Tenants []*models.Tenant `json:"tenants" jsonschema_description:"The returned tenants"`
}

type GetSchemaResp struct {
	Schema *models.Schema `json:"schema" jsonschema_description:"The returned schema"`
}
