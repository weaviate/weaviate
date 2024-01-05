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

package fixtures

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate-go-client/v4/weaviate"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
)

const (
	PIZZA_QUATTRO_FORMAGGI_ID = "10523cdd-15a2-42f4-81fa-267fe92f7cd6"
	PIZZA_FRUTTI_DI_MARE_ID   = "927dd3ac-e012-4093-8007-7799cc7e81e4"
	PIZZA_HAWAII_ID           = "00000000-0000-0000-0000-000000000000"
	PIZZA_DOENER_ID           = "5b6a08ba-1d46-43aa-89cc-8b070790c6f2"
	SOUP_CHICKENSOUP_ID       = "8c156d37-81aa-4ce9-a811-621e2702b825"
	SOUP_BEAUTIFUL_ID         = "27351361-2898-4d1a-aad7-1ca48253eb0b"
	SOUP_TRIPE_ID             = "a0b0c0d0-2898-4d1a-aad7-1ca48253eb0b"
	RISOTTO_RISI_E_BISI_ID    = "da751a25-f573-4715-a893-e607b2de0ba4"
	RISOTTO_ALLA_PILOTA_ID    = "10c2ee44-7d58-42be-9d64-5766883ca8cb"
	RISOTTO_AL_NERO_DI_SEPPIA = "696bf381-7f98-40a4-bcad-841780e00e0e"
)

var IdsByClass = map[string][]string{
	"Pizza": {
		PIZZA_QUATTRO_FORMAGGI_ID,
		PIZZA_FRUTTI_DI_MARE_ID,
		PIZZA_HAWAII_ID,
		PIZZA_DOENER_ID,
	},
	"Soup": {
		SOUP_CHICKENSOUP_ID,
		SOUP_BEAUTIFUL_ID,
	},
	"Risotto": {
		RISOTTO_RISI_E_BISI_ID,
		RISOTTO_ALLA_PILOTA_ID,
		RISOTTO_AL_NERO_DI_SEPPIA,
	},
}

var BeaconsByClass = map[string][]map[string]string{
	"Pizza": {
		{"beacon": makeBeacon("Pizza", PIZZA_QUATTRO_FORMAGGI_ID)},
		{"beacon": makeBeacon("Pizza", PIZZA_FRUTTI_DI_MARE_ID)},
		{"beacon": makeBeacon("Pizza", PIZZA_HAWAII_ID)},
		{"beacon": makeBeacon("Pizza", PIZZA_DOENER_ID)},
	},
	"Soup": {
		{"beacon": makeBeacon("Soup", SOUP_CHICKENSOUP_ID)},
		{"beacon": makeBeacon("Soup", SOUP_BEAUTIFUL_ID)},
	},
	"Risotto": {
		{"beacon": makeBeacon("Risotto", RISOTTO_RISI_E_BISI_ID)},
		{"beacon": makeBeacon("Risotto", RISOTTO_ALLA_PILOTA_ID)},
		{"beacon": makeBeacon("Risotto", RISOTTO_AL_NERO_DI_SEPPIA)},
	},
}

var AllIds = []string{
	PIZZA_QUATTRO_FORMAGGI_ID,
	PIZZA_FRUTTI_DI_MARE_ID,
	PIZZA_HAWAII_ID,
	PIZZA_DOENER_ID,

	SOUP_CHICKENSOUP_ID,
	SOUP_BEAUTIFUL_ID,

	RISOTTO_RISI_E_BISI_ID,
	RISOTTO_ALLA_PILOTA_ID,
	RISOTTO_AL_NERO_DI_SEPPIA,
}

func makeBeacon(class, id string) string {
	return fmt.Sprintf("weaviate://localhost/%s/%s", class, id)
}

// ##### SCHEMA #####

func CreateSchemaPizza(t *testing.T, client *weaviate.Client) {
	createSchema(t, client, classPizza())
}

func CreateSchemaSoup(t *testing.T, client *weaviate.Client) {
	createSchema(t, client, classSoup())
}

func CreateSchemaRisotto(t *testing.T, client *weaviate.Client) {
	createSchema(t, client, classRisotto())
}

func CreateSchemaFood(t *testing.T, client *weaviate.Client) {
	CreateSchemaPizza(t, client)
	CreateSchemaSoup(t, client)
	CreateSchemaRisotto(t, client)
}

func CreateSchemaPizzaForTenants(t *testing.T, client *weaviate.Client) {
	createSchema(t, client, classForTenants(classPizza()))
}

func CreateSchemaSoupForTenants(t *testing.T, client *weaviate.Client) {
	createSchema(t, client, classForTenants(classSoup()))
}

func CreateSchemaRisottoForTenants(t *testing.T, client *weaviate.Client) {
	createSchema(t, client, classForTenants(classRisotto()))
}

func CreateSchemaFoodForTenants(t *testing.T, client *weaviate.Client) {
	CreateSchemaPizzaForTenants(t, client)
	CreateSchemaSoupForTenants(t, client)
	CreateSchemaRisottoForTenants(t, client)
}

func createSchema(t *testing.T, client *weaviate.Client, class *models.Class) {
	err := client.Schema().ClassCreator().
		WithClass(class).
		Do(context.Background())

	require.Nil(t, err)
}

// ##### CLASSES #####

func classPizza() *models.Class {
	return &models.Class{
		Class:               "Pizza",
		Description:         "A delicious religion like food and arguably the best export of Italy.",
		InvertedIndexConfig: &models.InvertedIndexConfig{IndexTimestamps: true},
		Properties:          classPropertiesFood(),
	}
}

func classSoup() *models.Class {
	return &models.Class{
		Class:       "Soup",
		Description: "Mostly water based brew of sustenance for humans.",
		Properties:  classPropertiesFood(),
	}
}

func classRisotto() *models.Class {
	return &models.Class{
		Class:               "Risotto",
		Description:         "Risotto is a northern Italian rice dish cooked with broth.",
		InvertedIndexConfig: &models.InvertedIndexConfig{IndexTimestamps: true},
		Properties:          classPropertiesFood(),
	}
}

func classForTenants(class *models.Class) *models.Class {
	class.MultiTenancyConfig = &models.MultiTenancyConfig{
		Enabled: true,
	}
	return class
}

func classPropertiesFood() []*models.Property {
	nameProperty := &models.Property{
		Name:         "name",
		Description:  "name",
		DataType:     schema.DataTypeText.PropString(),
		Tokenization: models.PropertyTokenizationField,
	}
	descriptionProperty := &models.Property{
		Name:        "description",
		Description: "description",
		DataType:    schema.DataTypeText.PropString(),
	}
	bestBeforeProperty := &models.Property{
		Name:        "best_before",
		Description: "You better eat this food before it expires",
		DataType:    schema.DataTypeDate.PropString(),
	}
	priceProperty := &models.Property{
		Name:        "price",
		Description: "price",
		DataType:    schema.DataTypeNumber.PropString(),
		ModuleConfig: map[string]interface{}{
			"text2vec-contextionary": map[string]interface{}{
				"skip": true,
			},
		},
	}

	return []*models.Property{
		nameProperty, descriptionProperty, bestBeforeProperty, priceProperty,
	}
}

// ##### DATA #####

func CreateDataPizza(t *testing.T, client *weaviate.Client) {
	createData(t, client, []*models.Object{
		objectPizzaQuattroFormaggi(),
		objectPizzaFruttiDiMare(),
		objectPizzaHawaii(),
		objectPizzaDoener(),
	})
}

func CreateDataSoup(t *testing.T, client *weaviate.Client) {
	createData(t, client, []*models.Object{
		objectSoupChicken(),
		objectSoupBeautiful(),
	})
}

func CreateDataRisotto(t *testing.T, client *weaviate.Client) {
	createData(t, client, []*models.Object{
		objectRisottoRisiEBisi(),
		objectRisottoAllaPilota(),
		objectRisottoAlNeroDiSeppia(),
	})
}

func CreateDataFood(t *testing.T, client *weaviate.Client) {
	createData(t, client, []*models.Object{
		objectPizzaQuattroFormaggi(),
		objectPizzaFruttiDiMare(),
		objectPizzaHawaii(),
		objectPizzaDoener(),

		objectSoupChicken(),
		objectSoupBeautiful(),

		objectRisottoRisiEBisi(),
		objectRisottoAllaPilota(),
		objectRisottoAlNeroDiSeppia(),
	})
}

func createData(t *testing.T, client *weaviate.Client, objects []*models.Object) {
	resp, err := client.Batch().ObjectsBatcher().
		WithObjects(objects...).
		Do(context.Background())

	require.Nil(t, err)
	require.NotNil(t, resp)
	require.Len(t, resp, len(objects))
}

func CreateDataPizzaQuattroFormaggiForTenants(t *testing.T, client *weaviate.Client, tenantNames ...string) {
	createDataForTenants(t, client, tenantNames, func() []*models.Object {
		return []*models.Object{
			objectPizzaQuattroFormaggi(),
		}
	})
}

func CreateDataPizzaFruttiDiMareForTenants(t *testing.T, client *weaviate.Client, tenantNames ...string) {
	createDataForTenants(t, client, tenantNames, func() []*models.Object {
		return []*models.Object{
			objectPizzaFruttiDiMare(),
		}
	})
}

func CreateDataPizzaHawaiiForTenants(t *testing.T, client *weaviate.Client, tenantNames ...string) {
	createDataForTenants(t, client, tenantNames, func() []*models.Object {
		return []*models.Object{
			objectPizzaHawaii(),
		}
	})
}

func CreateDataPizzaDoenerForTenants(t *testing.T, client *weaviate.Client, tenantNames ...string) {
	createDataForTenants(t, client, tenantNames, func() []*models.Object {
		return []*models.Object{
			objectPizzaDoener(),
		}
	})
}

func CreateDataPizzaForTenants(t *testing.T, client *weaviate.Client, tenantNames ...string) {
	createDataForTenants(t, client, tenantNames, func() []*models.Object {
		return []*models.Object{
			objectPizzaQuattroFormaggi(),
			objectPizzaFruttiDiMare(),
			objectPizzaHawaii(),
			objectPizzaDoener(),
		}
	})
}

func CreateDataSoupChickenForTenants(t *testing.T, client *weaviate.Client, tenantNames ...string) {
	createDataForTenants(t, client, tenantNames, func() []*models.Object {
		return []*models.Object{
			objectSoupChicken(),
		}
	})
}

func CreateDataSoupBeautifulForTenants(t *testing.T, client *weaviate.Client, tenantNames ...string) {
	createDataForTenants(t, client, tenantNames, func() []*models.Object {
		return []*models.Object{
			objectSoupBeautiful(),
		}
	})
}

func CreateDataSoupForTenants(t *testing.T, client *weaviate.Client, tenantNames ...string) {
	createDataForTenants(t, client, tenantNames, func() []*models.Object {
		return []*models.Object{
			objectSoupChicken(),
			objectSoupBeautiful(),
		}
	})
}

func CreateDataRisottoForTenants(t *testing.T, client *weaviate.Client, tenantNames ...string) {
	createDataForTenants(t, client, tenantNames, func() []*models.Object {
		return []*models.Object{
			objectRisottoRisiEBisi(),
			objectRisottoAllaPilota(),
			objectRisottoAlNeroDiSeppia(),
		}
	})
}

func CreateDataFoodForTenants(t *testing.T, client *weaviate.Client, tenantNames ...string) {
	createDataForTenants(t, client, tenantNames, func() []*models.Object {
		return []*models.Object{
			objectPizzaQuattroFormaggi(),
			objectPizzaFruttiDiMare(),
			objectPizzaHawaii(),
			objectPizzaDoener(),

			objectSoupChicken(),
			objectSoupBeautiful(),

			objectRisottoRisiEBisi(),
			objectRisottoAllaPilota(),
			objectRisottoAlNeroDiSeppia(),
		}
	})
}

func createDataForTenants(t *testing.T, client *weaviate.Client,
	tenantNames []string, objectsSupplier func() []*models.Object,
) {
	objects := []*models.Object{}
	for _, name := range tenantNames {
		for _, object := range objectsSupplier() {
			object.Tenant = name
			objects = append(objects, object)
		}
	}
	createData(t, client, objects)
}

// ##### OBJECTS #####

func objectPizzaQuattroFormaggi() *models.Object {
	return &models.Object{
		Class: "Pizza",
		ID:    PIZZA_QUATTRO_FORMAGGI_ID,
		Properties: map[string]interface{}{
			"name":        "Quattro Formaggi",
			"description": "Pizza quattro formaggi Italian: [ˈkwattro forˈmaddʒi] (four cheese pizza) is a variety of pizza in Italian cuisine that is topped with a combination of four kinds of cheese, usually melted together, with (rossa, red) or without (bianca, white) tomato sauce. It is popular worldwide, including in Italy,[1] and is one of the iconic items from pizzerias's menus.",
			"price":       float32(1.1),
			"best_before": "2022-05-03T12:04:40+02:00",
		},
	}
}

func objectPizzaFruttiDiMare() *models.Object {
	return &models.Object{
		Class: "Pizza",
		ID:    PIZZA_FRUTTI_DI_MARE_ID,
		Properties: map[string]interface{}{
			"name":        "Frutti di Mare",
			"description": "Frutti di Mare is an Italian type of pizza that may be served with scampi, mussels or squid. It typically lacks cheese, with the seafood being served atop a tomato sauce.",
			"price":       float32(1.2),
			"best_before": "2022-05-05T07:16:30+02:00",
		},
	}
}

func objectPizzaHawaii() *models.Object {
	return &models.Object{
		Class: "Pizza",
		// this uuid guarantees that it's the first for cursor tests (otherwise
		// they might be flaky if the randomly generated ids are sometimes higher
		// and sometimes lower
		ID: PIZZA_HAWAII_ID,
		Properties: map[string]interface{}{
			"name":        "Hawaii",
			"description": "Universally accepted to be the best pizza ever created.",
			"price":       float32(1.3),
		},
	}
}

func objectPizzaDoener() *models.Object {
	return &models.Object{
		Class: "Pizza",
		ID:    PIZZA_DOENER_ID,
		Properties: map[string]interface{}{
			"name":        "Doener",
			"description": "A innovation, some say revolution, in the pizza industry.",
			"price":       float32(1.4),
		},
	}
}

func objectSoupChicken() *models.Object {
	return &models.Object{
		Class: "Soup",
		ID:    SOUP_CHICKENSOUP_ID,
		Properties: map[string]interface{}{
			"name":        "ChickenSoup",
			"description": "Used by humans when their inferior genetics are attacked by microscopic organisms.",
			"price":       float32(2.1),
		},
	}
}

func objectSoupBeautiful() *models.Object {
	return &models.Object{
		Class: "Soup",
		ID:    SOUP_BEAUTIFUL_ID,
		Properties: map[string]interface{}{
			"name":        "Beautiful",
			"description": "Putting the game of letter soups to a whole new level.",
			"price":       float32(2.2),
		},
	}
}

func objectRisottoRisiEBisi() *models.Object {
	return &models.Object{
		Class: "Risotto",
		ID:    RISOTTO_RISI_E_BISI_ID,
		Properties: map[string]interface{}{
			"name":        "Risi e bisi",
			"description": "A Veneto spring dish.",
			"price":       float32(3.1),
			"best_before": "2022-05-03T12:04:40+02:00",
		},
	}
}

func objectRisottoAllaPilota() *models.Object {
	return &models.Object{
		Class: "Risotto",
		ID:    RISOTTO_ALLA_PILOTA_ID,
		Properties: map[string]interface{}{
			"name":        "Risotto alla pilota",
			"description": "A specialty of Mantua, made with sausage, pork, and Parmesan cheese.",
			"price":       float32(3.2),
			"best_before": "2022-05-03T12:04:40+02:00",
		},
	}
}

func objectRisottoAlNeroDiSeppia() *models.Object {
	return &models.Object{
		Class: "Risotto",
		ID:    RISOTTO_AL_NERO_DI_SEPPIA,
		Properties: map[string]interface{}{
			"name":        "Risotto al nero di seppia",
			"description": "A specialty of the Veneto region, made with cuttlefish cooked with their ink-sacs intact, leaving the risotto black .",
			"price":       float32(3.3),
		},
	}
}

// ##### TENANTS #####

func CreateTenantsPizza(t *testing.T, client *weaviate.Client, tenants ...models.Tenant) {
	createTenants(t, client, "Pizza", tenants)
}

func CreateTenantsSoup(t *testing.T, client *weaviate.Client, tenants ...models.Tenant) {
	createTenants(t, client, "Soup", tenants)
}

func CreateTenantsRisotto(t *testing.T, client *weaviate.Client, tenants ...models.Tenant) {
	createTenants(t, client, "Risotto", tenants)
}

func CreateTenantsFood(t *testing.T, client *weaviate.Client, tenants ...models.Tenant) {
	CreateTenantsPizza(t, client, tenants...)
	CreateTenantsSoup(t, client, tenants...)
	CreateTenantsRisotto(t, client, tenants...)
}

func createTenants(t *testing.T, client *weaviate.Client, className string, tenants []models.Tenant) {
	err := client.Schema().TenantsCreator().
		WithClassName(className).
		WithTenants(tenants...).
		Do(context.Background())

	require.Nil(t, err)
}

type Tenants []models.Tenant

func (t Tenants) Names() []string {
	names := make([]string, len(t))
	for i, tenant := range t {
		names[i] = tenant.Name
	}
	return names
}

func (t Tenants) ByName(name string) *models.Tenant {
	for _, tenant := range t {
		if tenant.Name == name {
			return &tenant
		}
	}
	return nil
}
