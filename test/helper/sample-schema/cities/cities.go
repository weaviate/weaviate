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

package cities

import (
	"fmt"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/schema/crossref"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/usecases/config"
)

const (
	Country = "Country"
	City    = "City"
	Airport = "Airport"
)

const (
	Netherlands   strfmt.UUID = "67b79643-cf8b-4b22-b206-6e63dbb4e57a"
	Germany       strfmt.UUID = "561eea29-b733-4079-b50b-cfabd78190b7"
	Amsterdam     strfmt.UUID = "8f5f8e44-d348-459c-88b1-c1a44bb8f8be"
	Rotterdam     strfmt.UUID = "660db307-a163-41d2-8182-560782cd018f"
	Berlin        strfmt.UUID = "9b9cbea5-e87e-4cd0-89af-e2f424fd52d6"
	Dusseldorf    strfmt.UUID = "6ffb03f8-a853-4ec5-a5d8-302e45aaaf13"
	Missingisland strfmt.UUID = "823abeca-eef3-41c7-b587-7a6977b08003"
	Nullisland    strfmt.UUID = "823abeca-eef3-41c7-b587-7a6977b08067"
	Airport1      strfmt.UUID = "4770bb19-20fd-406e-ac64-9dac54c27a0f"
	Airport2      strfmt.UUID = "cad6ab9b-5bb9-4388-a933-a5bdfd23db37"
	Airport3      strfmt.UUID = "55a4dbbb-e2af-4b2a-901d-98146d1eeca7"
	Airport4      strfmt.UUID = "62d15920-b546-4844-bc87-3ae33268fab5"
)

const (
	HistoryAmsterdam  = "Due to its geographical location in what used to be wet peatland, the founding of Amsterdam is of a younger age than the founding of other urban centers in the Low Countries. However, in and around the area of what later became Amsterdam, local farmers settled as early as three millennia ago. They lived along the prehistoric IJ river and upstream of its tributary Amstel. The prehistoric IJ was a shallow and quiet stream in peatland behind beach ridges. This secluded area could grow there into an important local settlement center, especially in the late Bronze Age, the Iron Age and the Roman Age. Neolithic and Roman artefacts have also been found downstream of this area, in the prehistoric Amstel bedding under Amsterdam's Damrak and Rokin, such as shards of Bell Beaker culture pottery (2200-2000 BC) and a granite grinding stone (2700-2750 BC).[27][28] But the location of these artefacts around the river banks of the Amstel probably point to a presence of a modest semi-permanent or seasonal settlement of the previous mentioned local farmers. A permanent settlement would not have been possible, since the river mouth and the banks of the Amstel in this period in time were too wet for permanent habitation"
	HistoryRotterdam  = "On 7 July 1340, Count Willem IV of Holland granted city rights to Rotterdam, whose population then was only a few thousand.[14] Around the year 1350, a shipping canal (the Rotterdamse Schie) was completed, which provided Rotterdam access to the larger towns in the north, allowing it to become a local trans-shipment centre between the Netherlands, England and Germany, and to urbanize"
	HistoryBerlin     = "The earliest evidence of settlements in the area of today's Berlin are remnants of a house foundation dated to 1174, found in excavations in Berlin Mitte,[27] and a wooden beam dated from approximately 1192.[28] The first written records of towns in the area of present-day Berlin date from the late 12th century. Spandau is first mentioned in 1197 and Köpenick in 1209, although these areas did not join Berlin until 1920.[29] The central part of Berlin can be traced back to two towns. Cölln on the Fischerinsel is first mentioned in a 1237 document, and Berlin, across the Spree in what is now called the Nikolaiviertel, is referenced in a document from 1244.[28] 1237 is considered the founding date of the city.[30] The two towns over time formed close economic and social ties, and profited from the staple right on the two important trade routes Via Imperii and from Bruges to Novgorod.[12] In 1307, they formed an alliance with a common external policy, their internal administrations still being separated"
	HistoryDusseldorf = "The first written mention of Düsseldorf (then called Dusseldorp in the local Low Rhenish dialect) dates back to 1135. Under Emperor Friedrich Barbarossa the small town of Kaiserswerth to the north of Düsseldorf became a well-fortified outpost, where soldiers kept a watchful eye on every movement on the Rhine. Kaiserswerth eventually became a suburb of Düsseldorf in 1929. In 1186, Düsseldorf came under the rule of the Counts of Berg. 14 August 1288 is one of the most important dates in the history of Düsseldorf. On this day the sovereign Count Adolf VIII of Berg granted the village on the banks of the Düssel town privileges. Before this, a bloody struggle for power had taken place between the Archbishop of Cologne and the count of Berg, culminating in the Battle of Worringen"
)

func CreateCountryCityAirportSchema(t *testing.T, host string) {
	helper.SetupClient(host)
	helper.CreateClass(t, &models.Class{
		Class:      Country,
		Vectorizer: "text2vec-contextionary",
		ModuleConfig: map[string]interface{}{
			"text2vec-contextionary": map[string]interface{}{
				"vectorizeClassName": true,
			},
		},
		Properties: []*models.Property{
			{
				Name:         "name",
				DataType:     schema.DataTypeText.PropString(),
				Tokenization: models.PropertyTokenizationWhitespace,
			},
		},
	})

	// City class has only one vectorizable field: "name"
	// the rest of the fields are explicitly set to skip vectorization
	// to not to downgrade the distance/certainty result on which the
	// aggregate tests are based on.
	helper.CreateClass(t, &models.Class{
		Class:      City,
		Vectorizer: "text2vec-contextionary",
		ModuleConfig: map[string]interface{}{
			"text2vec-contextionary": map[string]interface{}{
				"vectorizeClassName": true,
			},
		},
		InvertedIndexConfig: &models.InvertedIndexConfig{IndexNullState: true, IndexPropertyLength: true, IndexTimestamps: true, UsingBlockMaxWAND: config.DefaultUsingBlockMaxWAND},
		Properties: []*models.Property{
			{
				Name:         "name",
				DataType:     schema.DataTypeText.PropString(),
				Tokenization: models.PropertyTokenizationWhitespace,
				ModuleConfig: map[string]interface{}{
					"text2vec-contextionary": map[string]interface{}{
						"skip": false,
					},
				},
			},
			{
				Name:     "inCountry",
				DataType: []string{Country},
				ModuleConfig: map[string]interface{}{
					"text2vec-contextionary": map[string]interface{}{
						"skip": true,
					},
				},
			},
			{
				Name:     "population",
				DataType: []string{"int"},
				ModuleConfig: map[string]interface{}{
					"text2vec-contextionary": map[string]interface{}{
						"skip": true,
					},
				},
			},
			{
				Name:     "location",
				DataType: []string{"geoCoordinates"},
				ModuleConfig: map[string]interface{}{
					"text2vec-contextionary": map[string]interface{}{
						"skip": true,
					},
				},
			},
			{
				Name:     "isCapital",
				DataType: []string{"boolean"},
				ModuleConfig: map[string]interface{}{
					"text2vec-contextionary": map[string]interface{}{
						"skip": true,
					},
				},
			},
			{
				Name:     "cityArea",
				DataType: []string{"number"},
				ModuleConfig: map[string]interface{}{
					"text2vec-contextionary": map[string]interface{}{
						"skip": true,
					},
				},
			},
			{
				Name:     "cityRights",
				DataType: []string{"date"},
				ModuleConfig: map[string]interface{}{
					"text2vec-contextionary": map[string]interface{}{
						"skip": true,
					},
				},
			},
			{
				Name:         "timezones",
				DataType:     schema.DataTypeTextArray.PropString(),
				Tokenization: models.PropertyTokenizationWhitespace,
				ModuleConfig: map[string]interface{}{
					"text2vec-contextionary": map[string]interface{}{
						"skip": true,
					},
				},
			},
			{
				Name:     "museums",
				DataType: []string{"text[]"},
				ModuleConfig: map[string]interface{}{
					"text2vec-contextionary": map[string]interface{}{
						"skip": true,
					},
				},
			},
			{
				Name:     "history",
				DataType: []string{"text"},
				ModuleConfig: map[string]interface{}{
					"text2vec-contextionary": map[string]interface{}{
						"skip": true,
					},
				},
			},
			{
				Name:     "phoneNumber",
				DataType: []string{"phoneNumber"},
				ModuleConfig: map[string]interface{}{
					"text2vec-contextionary": map[string]interface{}{
						"skip": true,
					},
				},
			},
		},
	})

	helper.CreateClass(t, &models.Class{
		Class:      Airport,
		Vectorizer: "text2vec-contextionary",
		ModuleConfig: map[string]interface{}{
			"text2vec-contextionary": map[string]interface{}{
				"vectorizeClassName": true,
			},
		},
		InvertedIndexConfig: &models.InvertedIndexConfig{
			CleanupIntervalSeconds: 60,
			Stopwords: &models.StopwordConfig{
				Preset: "en",
			},
			IndexTimestamps:   true,
			UsingBlockMaxWAND: config.DefaultUsingBlockMaxWAND,
		},
		Properties: []*models.Property{
			{
				Name:         "code",
				DataType:     schema.DataTypeText.PropString(),
				Tokenization: models.PropertyTokenizationWhitespace,
			},
			{
				Name:     "phone",
				DataType: []string{"phoneNumber"},
			},
			{
				Name:     "inCity",
				DataType: []string{City},
			},
			{
				Name:     "airportId",
				DataType: []string{"uuid"},
			},
		},
	})
}

func InsertCountryCityAirportObjects(t *testing.T, host string) {
	helper.SetupClient(host)
	// countries
	helper.CreateObject(t, &models.Object{
		Class: Country,
		ID:    Netherlands,
		Properties: map[string]interface{}{
			"name": "Netherlands",
		},
	})
	helper.CreateObject(t, &models.Object{
		Class: Country,
		ID:    Germany,
		Properties: map[string]interface{}{
			"name": "Germany",
		},
	})

	// cities
	helper.CreateObject(t, &models.Object{
		Class: City,
		ID:    Amsterdam,
		Properties: map[string]interface{}{
			"name":       "Amsterdam",
			"population": 1800000,
			"location": map[string]interface{}{
				"latitude":  52.366667,
				"longitude": 4.9,
			},
			"inCountry": []interface{}{
				map[string]interface{}{
					"beacon": crossref.NewLocalhost(Country, Netherlands).String(),
				},
			},
			"isCapital":  true,
			"cityArea":   float64(891.95),
			"cityRights": mustParseYear("1400"),
			"timezones":  []string{"CET", "CEST"},
			"museums":    []string{"Stedelijk Museum", "Rijksmuseum"},
			"history":    HistoryAmsterdam,
			"phoneNumber": map[string]interface{}{
				"input": "+311000004",
			},
		},
	})
	helper.CreateObject(t, &models.Object{
		Class: City,
		ID:    Rotterdam,
		Properties: map[string]interface{}{
			"name":       "Rotterdam",
			"population": 600000,
			"inCountry": []interface{}{
				map[string]interface{}{
					"beacon": crossref.NewLocalhost(Country, Netherlands).String(),
				},
			},
			"isCapital":  false,
			"cityArea":   float64(319.35),
			"cityRights": mustParseYear("1283"),
			"timezones":  []string{"CET", "CEST"},
			"museums":    []string{"Museum Boijmans Van Beuningen", "Wereldmuseum", "Witte de With Center for Contemporary Art"},
			"history":    HistoryRotterdam,
			"phoneNumber": map[string]interface{}{
				"input": "+311000000",
			},
		},
	})
	helper.CreateObject(t, &models.Object{
		Class: City,
		ID:    Berlin,
		Properties: map[string]interface{}{
			"name":       "Berlin",
			"population": 3470000,
			"inCountry": []interface{}{
				map[string]interface{}{
					"beacon": crossref.NewLocalhost(Country, Germany).String(),
				},
			},
			"isCapital":  true,
			"cityArea":   float64(891.96),
			"cityRights": mustParseYear("1400"),
			"timezones":  []string{"CET", "CEST"},
			"museums":    []string{"German Historical Museum"},
			"history":    HistoryBerlin,
			"phoneNumber": map[string]interface{}{
				"input": "+311000002",
			},
		},
	})
	helper.CreateObject(t, &models.Object{
		Class: City,
		ID:    Dusseldorf,
		Properties: map[string]interface{}{
			"name":       "Dusseldorf",
			"population": 600000,
			"inCountry": []interface{}{
				map[string]interface{}{
					"beacon": crossref.NewLocalhost(Country, Germany).String(),
				},
			},
			"location": map[string]interface{}{
				"latitude":  51.225556,
				"longitude": 6.782778,
			},
			"isCapital":  false,
			"cityArea":   float64(217.22),
			"cityRights": mustParseYear("1135"),
			"timezones":  []string{"CET", "CEST"},
			"museums":    []string{"Schlossturm", "Schiffahrt Museum", "Onomato"},
			"history":    HistoryDusseldorf,
			"phoneNumber": map[string]interface{}{
				"input": "+311000001",
			},
		},
	})

	helper.CreateObject(t, &models.Object{
		Class: City,
		ID:    Missingisland,
		Properties: map[string]interface{}{
			"name":       "Missing Island",
			"population": 0,
			"location": map[string]interface{}{
				"latitude":  0,
				"longitude": 0,
			},
			"isCapital": false,
		},
	})

	helper.CreateObject(t, &models.Object{
		Class: City,
		ID:    Nullisland,
		Properties: map[string]interface{}{
			"name":        nil,
			"population":  nil,
			"inCountry":   nil,
			"location":    nil,
			"isCapital":   nil,
			"cityArea":    nil,
			"cityRights":  nil,
			"timezones":   nil,
			"museums":     nil,
			"history":     nil,
			"phoneNumber": nil,
		},
	})

	// airports
	helper.CreateObject(t, &models.Object{
		Class: Airport,
		ID:    Airport1,
		Properties: map[string]interface{}{
			"code":      "10000",
			"airportId": uuid.MustParse("00000000-0000-0000-0000-000000010000").String(),
			"phone": map[string]interface{}{
				"input": "+311234567",
			},
			"inCity": []interface{}{
				map[string]interface{}{
					"beacon": crossref.NewLocalhost(City, Amsterdam).String(),
				},
			},
		},
	})
	helper.CreateObject(t, &models.Object{
		Class: Airport,
		ID:    Airport2,
		Properties: map[string]interface{}{
			"code":      "20000",
			"airportId": uuid.MustParse("00000000-0000-0000-0000-000000020000").String(),
			"inCity": []interface{}{
				map[string]interface{}{
					"beacon": crossref.NewLocalhost(City, Rotterdam).String(),
				},
			},
		},
	})
	helper.CreateObject(t, &models.Object{
		Class: Airport,
		ID:    Airport3,
		Properties: map[string]interface{}{
			"code":      "30000",
			"airportId": uuid.MustParse("00000000-0000-0000-0000-000000030000").String(),
			"inCity": []interface{}{
				map[string]interface{}{
					"beacon": crossref.NewLocalhost(City, Dusseldorf).String(),
				},
			},
		},
	})
	helper.CreateObject(t, &models.Object{
		Class: Airport,
		ID:    Airport4,
		Properties: map[string]interface{}{
			"code":      "40000",
			"airportId": uuid.MustParse("00000000-0000-0000-0000-000000040000").String(),
			"inCity": []interface{}{
				map[string]interface{}{
					"beacon": crossref.NewLocalhost(City, Berlin).String(),
				},
			},
		},
	})
}

func DeleteCountryCityAirportSchema(t *testing.T, host string) {
	helper.SetupClient(host)
	helper.DeleteClassObject(t, Airport)
	helper.DeleteClassObject(t, City)
	helper.DeleteClassObject(t, Country)
}

func mustParseYear(year string) time.Time {
	date := fmt.Sprintf("%s-01-01T00:00:00+02:00", year)
	asTime, err := time.Parse(time.RFC3339, date)
	if err != nil {
		panic(err)
	}
	return asTime
}
