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

package companies

import "github.com/go-openapi/strfmt"

const (
	OpenAI strfmt.UUID = "00000000-0000-0000-0000-000000000001"
	SpaceX strfmt.UUID = "00000000-0000-0000-0000-000000000002"
)

type Company struct {
	ID                strfmt.UUID
	Name, Description string
}

func Companies() []Company {
	return []Company{
		{
			ID:   OpenAI,
			Name: "OpenAI",
			Description: `OpenAI is a research organization and AI development company that focuses on artificial intelligence (AI) and machine learning (ML).
				Founded in December 2015, OpenAI's mission is to ensure that artificial general intelligence (AGI) benefits all of humanity.
				The organization has been at the forefront of AI research, producing cutting-edge advancements in natural language processing,
				reinforcement learning, robotics, and other AI-related fields.

				OpenAI has garnered attention for its work on various projects, including the development of the GPT (Generative Pre-trained Transformer)
				series of models, such as GPT-2 and GPT-3, which have demonstrated remarkable capabilities in generating human-like text.
				Additionally, OpenAI has contributed to advancements in reinforcement learning through projects like OpenAI Five, an AI system
				capable of playing the complex strategy game Dota 2 at a high level.`,
		},
		{
			ID:   SpaceX,
			Name: "SpaceX",
			Description: `SpaceX, short for Space Exploration Technologies Corp., is an American aerospace manufacturer and space transportation company
				founded by Elon Musk in 2002. The company's primary goal is to reduce space transportation costs and enable the colonization of Mars,
				among other ambitious objectives.

				SpaceX has made significant strides in the aerospace industry by developing advanced rocket technology, spacecraft,
				and satellite systems. The company is best known for its Falcon series of rockets, including the Falcon 1, Falcon 9, 
				and Falcon Heavy, which have been designed with reusability in mind. Reusability has been a key innovation pioneered by SpaceX,
				aiming to drastically reduce the cost of space travel by reusing rocket components multiple times.`,
		},
	}
}
