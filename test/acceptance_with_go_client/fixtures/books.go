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

type Book struct {
	Author, Title, Description, Genre string
}

func Books() map[string]Book {
	books := map[string]Book{
		"00000000-0000-0000-0000-000000000001": {
			Author:      "J.R.R. Tolkien",
			Title:       "The Lord of the rings",
			Description: `"The Lord of the Rings" is a timeless epic fantasy tale penned by J.R.R. Tolkien. Set in the fictional world of Middle-earth, the story follows the quest of Frodo Baggins, a humble hobbit, who is tasked with destroying a powerful and corrupting artifact, the One Ring. Accompanied by a diverse fellowship of companions, including humans, elves, dwarves, and other hobbits, Frodo embarks on a perilous journey across Middle-earth to Mount Doom, the only place where the Ring can be unmade. Along the way, they encounter ancient evils, forge unbreakable bonds of friendship, and face monumental challenges that will test their courage and resolve. As the fate of Middle-earth hangs in the balance, Frodo and his companions must confront the forces of darkness in an epic struggle for the survival of their world. Rich with themes of friendship, sacrifice, and the eternal battle between good and evil, "The Lord of the Rings" remains one of the most beloved and influential works of fantasy literature ever written.`,
			Genre:       "fantasy",
		},
		"00000000-0000-0000-0000-000000000002": {
			Author:      "Douglas Adams",
			Title:       "The Hitchhiker's Guide to the Galaxy",
			Description: `"The Hitchhiker's Guide to the Galaxy" is a comedic science fiction novel written by Douglas Adams. It follows the misadventures of Arthur Dent, an ordinary human who is unexpectedly whisked away from Earth just before its demolition to make way for a hyperspace bypass. Arthur's friend, Ford Prefect, turns out to be an alien researcher for the titular guidebook, a humorous compendium of knowledge for intergalactic travelers. Together, they embark on a journey through space, encountering a quirky cast of characters including the two-headed, three-armed ex-President of the Galaxy, Zaphod Beeblebrox, and the depressed robot Marvin. Filled with absurdity, wit, and biting satire, "The Hitchhiker's Guide to the Galaxy" is a delightful romp through the cosmos that pokes fun at the human condition and the universe at large.`,
			Genre:       "science-fiction",
		},
		"00000000-0000-0000-0000-000000000003": {
			Author:      "Frank Herbert",
			Title:       "Dune",
			Description: `"Dune" is a science fiction masterpiece written by Frank Herbert. Set in a distant future where noble houses vie for control of the desert planet Arrakis, known for its valuable spice melange, the story follows young Paul Atreides as his family assumes stewardship of Arrakis. Paul's journey intertwines with political intrigue, betrayal, and a prophesied destiny that could change the fate of the universe. As Paul navigates the harsh desert landscape and confronts the enigmatic Fremen people, he undergoes a transformation that will shape the course of history. "Dune" is a sweeping epic filled with intricate world-building, complex characters, and philosophical themes that have captivated readers for generations.`,
			Genre:       "science-fiction",
		},
		"00000000-0000-0000-0000-000000000004": {
			Author:      "Isaac Asimov",
			Title:       "Foundation",
			Description: `"Foundation" by Isaac Asimov is a groundbreaking science fiction novel that explores the concept of psychohistory—a mathematical science capable of predicting the future of large populations. Set in a distant future where the Galactic Empire is in decline, the story follows mathematician Hari Seldon, who predicts the imminent fall of civilization. To preserve knowledge and shorten the dark ages that will follow, Seldon establishes the Foundation—a secretive organization tasked with guiding humanity's destiny. Spanning centuries, "Foundation" chronicles the Foundation's efforts to navigate political intrigue, technological advancement, and the rise and fall of empires. Asimov's masterful storytelling and thought-provoking exploration of sociopolitical themes make "Foundation" a timeless classic of science fiction literature.`,
			Genre:       "science-fiction",
		},
		"00000000-0000-0000-0000-000000000005": {
			Author:      "Liu Cixin",
			Title:       "The Three-Body Problem",
			Description: `"The Three-Body Problem" by Liu Cixin is a groundbreaking science fiction novel that melds complex scientific concepts with compelling storytelling. Set against the backdrop of China's Cultural Revolution and its aftermath, the story follows physicist Ye Wenjie as she becomes embroiled in a secretive government project to make contact with extraterrestrial life. Meanwhile, present-day scientist Wang Miao finds himself drawn into a mysterious online game that holds the key to understanding an impending alien invasion. As humanity grapples with the implications of contact with an advanced alien civilization and the existential threats it poses, "The Three-Body Problem" explores themes of scientific discovery, cultural revolution, and the nature of humanity itself. Liu Cixin's visionary narrative and intricate exploration of physics and philosophy make "The Three-Body Problem" a thought-provoking and immersive journey into the unknown.`,
			Genre:       "science-fiction",
		},
		"00000000-0000-0000-0000-000000000006": {
			Author:      "Richard K. Morgan",
			Title:       "Altered Carbon",
			Description: `"Altered Carbon" by Richard K. Morgan is a gripping science fiction novel set in a future where consciousness can be transferred between bodies, known as "sleeves," and death has become nearly obsolete for the wealthy. The story follows Takeshi Kovacs, a former soldier turned private investigator, who is hired to solve the murder of a wealthy aristocrat, Laurens Bancroft. As Kovacs delves deeper into the seedy underbelly of a society where the rich live for centuries and the poor struggle to survive, he uncovers a conspiracy that threatens to unravel the fabric of society itself. Blending hard-boiled detective noir with cyberpunk elements and thought-provoking explorations of identity, morality, and the commodification of life, "Altered Carbon" is a thrilling and immersive exploration of a dystopian future where the line between humanity and technology has blurred beyond recognition.`,
			Genre:       "science-fiction",
		},
		"00000000-0000-0000-0000-000000000007": {
			Author:      "Jeff VanderMeer",
			Title:       "Borne",
			Description: `"Borne" by Jeff VanderMeer is a captivating and surreal science fiction novel set in a post-apocalyptic world devastated by biotech experiments gone awry. The story unfolds in a city overrun by strange and dangerous creatures, where scavenger Rachel discovers a mysterious organism she names Borne. As Rachel nurtures Borne, he rapidly grows and evolves, exhibiting intelligence and abilities beyond comprehension. However, their bond is tested when Borne's origins and true nature are revealed, sparking a journey of discovery and survival in a world where nothing is as it seems. VanderMeer's vivid imagination, lyrical prose, and thought-provoking exploration of humanity, nature, and the ethics of scientific experimentation make "Borne" a mesmerizing and unforgettable reading experience.`,
			Genre:       "science-fiction",
		},
		"00000000-0000-0000-0000-000000000008": {
			Author:      "Greg Egan",
			Title:       "Diaspora",
			Description: `"Diaspora" by Greg Egan is a mind-bending science fiction novel that explores themes of identity, consciousness, and the nature of existence in a universe where humanity has transcended physical limitations. Set in the distant future, the story follows a group of post-human beings known as "polises" who exist as digital entities in vast virtual environments. When a catastrophic event threatens their existence, a diverse cast of characters embarks on a journey of exploration and discovery across the cosmos, encountering enigmatic aliens and grappling with existential questions about the nature of reality. Egan's meticulous attention to scientific detail and thought-provoking speculation on the future evolution of humanity make "Diaspora" a challenging yet rewarding exploration of the boundaries of human potential in an infinitely expanding universe.`,
			Genre:       "science-fiction",
		},
		"00000000-0000-0000-0000-000000000009": {
			Author:      "Janusz A. Zajdel",
			Title:       "Limes inferior",
			Description: `"Limes Inferior" is a renowned novel written by Janusz A. Zajdel, a prominent Polish science fiction writer. Set in a dystopian future, the story takes place in a post-apocalyptic world where humanity's survival is threatened by ecological devastation and totalitarian regimes. The narrative follows the protagonist, Peter Cogito, as he navigates the oppressive society ruled by a powerful government and struggles to find his place in a world filled with corruption, surveillance, and social unrest. As Peter delves deeper into the secrets of his society, he uncovers dark truths about the nature of power and the consequences of unchecked authority. "Limes Inferior" is celebrated for its thought-provoking exploration of political themes, moral dilemmas, and the resilience of the human spirit in the face of adversity.`,
			Genre:       "science-fiction",
		},
		"00000000-0000-0000-0000-000000000010": {
			Author:      "Walter Tevis",
			Title:       "The Man Who Fell to Earth",
			Description: `"The Man Who Fell to Earth" is a thought-provoking science fiction novel written by Walter Tevis. The story revolves around Thomas Jerome Newton, an extraterrestrial being from a drought-stricken planet called Anthea. Disguised as a human, Newton arrives on Earth with advanced technology to save his dying world. He becomes a wealthy industrialist, using his knowledge to revolutionize various industries. However, as he becomes increasingly entangled in Earth's society, Newton grapples with the complexities of human emotions, desires, and vices. The novel explores themes of alienation, loneliness, and the struggle to maintain one's identity in a world vastly different from one's own. "The Man Who Fell to Earth" is a poignant and introspective tale that delves into the human condition and the universal quest for connection and belonging.`,
			Genre:       "science-fiction",
		},
		"00000000-0000-0000-0000-000000000011": {
			Author:      "Neal Stephenson",
			Title:       "Seveneves",
			Description: `"Seveneves" by Neal Stephenson is an epic science fiction novel that begins with the sudden destruction of the Moon, setting off a chain reaction of events that threatens all life on Earth. Facing imminent extinction, humanity races against time to ensure its survival by sending a select group of individuals into space to live aboard hastily constructed space habitats known as the "Cloud Ark." As the survivors grapple with the challenges of adaptation and coexistence in their new environment, tensions rise and alliances shift in the struggle to preserve humanity's future. Spanning thousands of years, "Seveneves" explores themes of resilience, ingenuity, and the endurance of the human spirit in the face of catastrophic adversity. With meticulous attention to scientific detail and grand scope, Neal Stephenson delivers a riveting tale of survival and evolution that transcends the bounds of time and space.`,
			Genre:       "science-fiction",
		},
		"00000000-0000-0000-0000-000000000012": {
			Author:      "Andy Weir",
			Title:       "Project Hail Mary",
			Description: `"Project Hail Mary" is a thrilling science fiction novel by Andy Weir, acclaimed author of "The Martian." The story follows Ryland Grace, a brilliant but solitary scientist who wakes up aboard a spacecraft with no memory of who he is or how he got there. As he pieces together his identity and the purpose of his mission, Ryland discovers that he is humanity's last hope to save Earth from an impending catastrophe. With the help of an unlikely ally, an extraterrestrial organism named Rocky, Ryland embarks on a perilous journey across the cosmos to uncover the truth behind the disaster and find a solution to save mankind. Filled with suspense, humor, and ingenious problem-solving, "Project Hail Mary" is a gripping tale of survival, discovery, and the indomitable human spirit in the face of unimaginable challenges.`,
			Genre:       "science-fiction",
		},
		"00000000-0000-0000-0000-000000000013": {
			Author:      "Dan Simmons",
			Title:       "Hyperion",
			Description: `"Hyperion" by Dan Simmons is a gripping science fiction novel that unfolds as a series of interconnected stories, each told by one of seven pilgrims on a journey to the distant world of Hyperion. Set in a future where humanity has spread across the galaxy, the pilgrims share their tales as they seek answers to the enigmatic Shrike, a legendary creature that resides on Hyperion. As each pilgrim reveals their past, fears, and desires, a larger narrative emerges—one of political intrigue, religious conflict, and existential dread. With rich world-building, complex characters, and a narrative structure reminiscent of Chaucer's "The Canterbury Tales," "Hyperion" is a compelling exploration of humanity's place in the universe, the nature of consciousness, and the search for meaning in an ever-expanding cosmos.`,
			Genre:       "science-fiction",
		},
		"00000000-0000-0000-0000-000000000014": {
			Author:      "Jacek Dukaj",
			Title:       "Black Oceans",
			Description: `"Black Oceans" is a science fiction novel by Polish writer Jacek Dukaj, published in Poland by Supernowa in 2001. The novel fits in the hard science fiction genre, describing the late-21st century Earth facing technological singularity. The novel received the prime Polish award for sci-fi literature, Janusz A. Zajdel Award, for 2001.`,
			Genre:       "science-fiction",
		},
	}
	return books
}
