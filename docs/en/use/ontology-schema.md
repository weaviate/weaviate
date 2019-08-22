> We are updating our docs and they will be moved to [www.semi.technology](https://www.semi.technology) soon.
> You can leave your email [here](http://eepurl.com/gye_bX) to get a notification when they are live.

# Weaviate Ontologies

> Designing and creating Weaviate ontologies.

Every Weaviate instance has an ontology which is used to describe what type of
data you are adding and relies heavily on the contextionary. The ontology also
helps other Weaviates in the P2P network to understand in which context your
data is being held.

## Things and Actions

Weaviate makes a conceptual distinction between things and actions. A thing is
described by a noun. For example, car, city, book, chair, etcetera are all
valid classes for things. Actions are verb-based, for example, move, bring,
build, etcetera are all valid classes for actions.

Within Weaviate you will define ontologies for both things and actions. When
starting a new Weaviate instance, you define the ontology first. A process you
can compare with defining columns and tables in a traditional database.

## Defining Classes

Classes are always written with a capital. For example: `Zoo`. If you want to
chain words, you can do this by using
[CamelCase](#camelcase-and-camelcase-in-class-and-property-names). For example:
`ZooCage` will be seen as a _zoo_ and _cage_.

Classes are defined as follows:

```json
{
  "class": "string",
  "description": "string",
  "keywords": [{
      "keyword": "string",
      "weight": 0
  }],
  "properties": []
}
```

Legend:

| key | value |
| --- | --- |
| class | A class is the CamelCased name of the thing or action. For example: `Zoo` or `Animal`. The class should be part of the [Weaviate Contextionary](../contribute/contextionary.md) |
| description | A short description of the class. |
| keywords | An array of keywords, more information can be found [here](#keywords) |
| properties | An array of [properties](#defining-properties). |

## Defining Properties

Every class has properties, properties are used to describe something inside
the class. The thing with the class _Animal_ can have a property _color_ for
example. And the action _move_ can have the property _agent_ to describe who or
what is moving. Properties are always written with a lowercase first character.
For example: `name`. If you want to chain words, you can do this by using
[camelCase](#camelcase-and-camelcase-in-class-and-property-names). For example:
`inZoo` will be seen as _in_ and _zoo_.

When defining a property, you also define what type the property has. It can be
a string or a number, but it can also contain a reference to another class or
even multiple classes. For example, if you have a class `Animal`, you might
create a property that is called `inCage`.

An overview of possible types.

Properties are defined as follows inside classes (based on the example above):

```json
{
	"class": "string",
	"description": "string",
	"keywords": [{
		"keyword": "string",
		"weight": 0
	}],
	"properties": [{
		"dataType": [
			"string"
		],
		"cardinality": "atMostOne",
		"description": "Name of the Zoo",
		"name": "name",
		"keywords": [{
			"keyword": "identifier",
			"weight": 0.01
		}]
	}]
}
```

Legend:

| key | value |
| --- | --- |
| properties.dataType | array |
| properties.dataType._string_ | the data type, see an overview of data types and formats [here](#property-datatypes) |
| properties.cardinality | Should be `atMostOne` or `many`, more information can be found [here](#cardinality) |
| properties.description | Description of the property |
| properties.name | The name of the property, this property should be part of the [Weaviate Contextionary](../contribute/contextionary.md) |
| properties.keywords | An array of keywords, more information can be found [here](#keywords) |

## Cardinality

A property's `dataType` is always set as one (`atMostOne`) meaning that it can
have only one type to direct to. However, when setting cross-references, you
sometimes want to be able to point to multiple things or actions.

For example, the class `Animal` might have the property `livesIn` which can be
a cross-reference to a cage or an aquarium. When using GraphQL to retrieve data
from the graph, the cardinality will determine how the query is constructed.

```json
{
    "class": "Animal",
    "description": "An animal in the zoo",
    "keywords": [],
    "properties": [{
        "name": "livesIn",
        "dataType": [
            "Aquarium", "Cage"
        ],
        "cardinality": "many",
        "description": "Where the animal lives",
        "keywords": []
    }]
}
```

## Keywords

Keywords give context to a class or property. They help a Weaviate instance to
interpret differnt words that are spelled the same way mean (so-called
homographs). A good example of this is the word `seal`. Do you mean a `stamp`
or the `sea animal`? You can define this by setting keywords. The weights
determine how important the keyword is. Setting low values is often already
enough to determine the context of a thing or action.

Example:

```json
...
"class": "Seal",
"description": "An individual seal",
"keywords": [
  {
    "keyword": "animal",
    "weight": 0.05
  },
  {
    "keyword": "sea",
    "weight": 0.01
  }
],
...
```

## CamelCase and camelCase in class and property names

Spaces are not allowed in either class or property names. However, you can
chain multiple words using `CamelCase` (or "upper camel case") for class names.
Similiarly you can use `camelCase` (or "lower camel case") to chain multiple
words together for property names.

If you don't camelCase or CamelCase the words, but instead write them as a
single word, such as `thisisasentenceofmanywords`, the keyword will most likely
fail the Contextionary validation, unless the combined word happens to be
present in the contextionary.

Keywords cannot be chained, they have to match exactly one word. However, there
is no limit on the amount of keywords per class or per class property.

Note that stopwords automatically removed from camelCased and CamelCased names,
see more in [the section about
stopwords](#stopwords-in-class-and-property-names).

### Examples

* For class names:
  * valid: `Zoo`, `AnimalZoo`, `ZooWithAnimals`,
    `VeryVerboseZooWithVerboseAnimals`
  * invalid: `animalzoo`, `animal zoo`, `animal Zoo`, `zoo-with-animals`,
    `ZooWithAnimales` (the last example is properly cased, but the typo in
    `Animal(e)s` is not a contextionary-valid word)
* For property names:
  * valid: `name`, `givenName`, `firstName`, `theNameThatThisPersonIsCalledBy`
  * invalid: `given Name`, `given name`, `givenname`, `given-name`,
    `given_name`, `given%$!*name`
* For keywords:
  * valid: `car`, `dealership`, `sales`, `person`, `selling`, `automobiles`
  * invalid: `carDealership`, `car dealership`, `sales-person`,
    `selling_automobiles`

## Stopwords in Class and Property Names

### What stopwords are and why they matter
Stopwords are words that don't add semantic meaning to your concepts and are
extremely common in texts across different contexts. For example, the sentence
"a car is parked on the street" contains the following stopwords: "a", "is",
"on", "the". If we look at the sentence "a banana is lying on
the table", you would find the exact same stop words. So in those two sentences
over 50% of the words overlap. Therefore they would be considered somewhat
similar (based on the overall vector position).

However, if we remove stopwords from both sentences, they become "car parked
street" and "banana lying table". Suddently there are 0% identical words in the
sentences, so it becomes easier to perform vector comparisons. Note at this
point we cannot say whether both sentences are related or not. For this we'd
need to know how close the vector position of the sentence "car parked street"
is to the vector position of "banana lying table". But we do know that the
result can now be calculated with a lot less noise.

### Behavior around stop words

Stopwords are useful for humans, so we don't want to encourage you to leave
them out completely. Instead weaviate will remove them whenever your schema
information is translated to vector positions.

In most cases you won't even notice that this happens in the background,
however, there are a few edge cases that might cause a validation error:

* If your camelCased class or property name consists **only** of stopwords,
  validation will fail. Example: `TheInA` is not a valid class name, however,
  `TheCarInAField` is (and would internally be represented as `CarField`).

* If your keyword list contains stop words, they will be removed. However, if
  every single keyword is a stop word, validation will fail.

### How does weaviate decide wether a word is a stop word or not?

The list of stopwords is derived from the contextionary version used and is
published alongside the contextionary files.

Check [c11y.semi.technology](c11y.semi.technology) or a list of stopwords using
the desired language and version. For example, to see the stopwords used in the
english language contextionary version 0.5.0, check
[https://c11y.semi.technology/0.5.0/en/stopwords.json](https://c11y.semi.technology/0.5.0/en/stopwords.json).


## Property Data Types

An overview of available data types:

| Weaviate Type | Exact Data Type | Formatting | Misc |
| ---------|--------|-----------| --- |
| string   | string | `string` |
| int      | int64  | `0` |
| boolean  | boolean | `true`/`false` |
| number   | float64 | `0.0` |
| date     | string | [ISO 8601](https://en.wikipedia.org/wiki/ISO_8601) |
| text     | text   | `string` | Used for large texts and is not queryable |
| CrossRef | string | [more info](#crossref-data-type) |
| geoCoordinates | string | [more info](#geoCoordinates-data-type) |

#### CrossRef Data Type

The crossref datatype consists of a URL type:

- **scheme** = `weaviate://`
- **host** = Weaviate P2P node, `localhost` for a local thing or action. Read more about Weaviate-P2P hosts [here](peer2peer-network.md).
- **path** = location of the thing or action.

Example: `weaviate://localhost/things/6406759e-f6fb-47ba-a537-1a62728d2f55`

In the [RESTful API](./RESTful.md) this will be shown as:

```json
{
    "thing": {
        "class": "SomeClass",
        "schema": {
            "name": "SomeOtherClass",
            "someProperty": {
                "beacon": "weaviate://localhost/things/6406759e-f6fb-47ba-a537-1a62728d2f55"
            }
        }
    }
}
```

#### geoCoordinates Data Type

Used for geo-coordinates in the dataset.

Example:

```json
{
  "class": "Animal",
  "description": "An animal in the zoo",
  "keywords": [],
  "properties": [{
    "name": "location",
    "dataType": [
        "geoCoordinates"
    ],
    "cardinality": "atMostOne",
    "description": "Where the animal is located",
    "keywords": []
  }]
}
```

Adding data:

```json
"Animal": {
  "geolocation": {
    "latitude": 52.366667,
    "longitude": 4.9
  }
}
```

## Example

A full blown example can be found
[here](https://github.com/semi-technologies/weaviate-demo-zoo). The [getting
started guide](./getting-started.md) also contains examples of creating an
ontology schema.
