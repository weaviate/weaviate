---
publishedOnWebsite: true
title: Ontology Schema
subject: OSS
---

# Ontology Schema

Every Weaviate instance has an ontology which is used to describe what type of data you are adding and relies heavily on the contextionary. The ontology also helps other Weaviates in the P2P network to understand in which context your data is being held.

## Things and Actions

Weaviate makes a conceptual distinction between things and actions. A thing is described as a noun. For example, car, city, book, chair, etcetera are all valid classes for things. Actions are verb-based, for example, move, bring, build, etcetera are all valid classes for actions.

Within Weaviate you will define ontologies for both things and actions. When starting a new weaviate instance, you define the ontology first. An endeavor you can compare to defining columns and tables in a traditional database.

## Properties

Every class has properties, properties are used to describe something inside the class. The thing _car_ can have a property _color_ for example. And the action _move_ can have the property _agent_ to describe who or what is moving.

When defining a property, you also define what type the property has. It can be a string or a number, but it can also contain a reference to another class or even multiple classes. For example, if you have a class `City`, you might create a property that is called `inCountry` . You can make `inCountry` a string, but it is of course way handier if you make it a cross-reference to a country. Also, see the examples below.

An overview of possible types.

```json
[{
    "name": "testString",
    "@dataType": [
      "string"
    ],
    "description": "Value of testString."
  },
  {
    "name": "testInt",
    "@dataType": [
      "int"
    ],
    "description": "Value of testInt."
  },
  {
    "name": "testBoolean",
    "@dataType": [
      "boolean"
    ],
    "description": "Value of testBoolean."
  },
  {
    "name": "testNumber",
    "@dataType": [
      "number"
    ],
    "description": "Value of testNumber."
  },
  {
    "name": "testDateTime",
    "@dataType": [
      "date"
    ],
    "description": "Value of testDateTime."
  },
  {
    "name": "testCref",
    "@dataType": [
      "TestThing2"
    ],
    "description": "Value of testCref."
  }
]
```

## Property datatypes

| Weaviate Type | Exact Data Type | Formatting |
| ---------|--------|-----------|
| string   | string | `string` |
| int      | int64  | `0` |
| boolean  | boolean | `true`/`false` |
| number   | float64 | `0.0` |
| date     | string | [ISO 8601](https://en.wikipedia.org/wiki/ISO_8601) |
| CrossRef | string | [CamelCase](#CamelCase) |

## Keywords & Context

Keywords give context to a class or property. They help a Weaviate instance to interpret what words which are written in the same way means (so-called homographs). A good example of this is the words `seal`. Do you refer to a `stamp` or the `sea animal`? You can define this by setting keywords.

Example:

```json
...
"class": "Place",
"description": "This is a place that people live in",
"keywords": [
  {
    "keyword": "city",
    "weight": 0.9
  },
  {
    "keyword": "town",
    "weight": 0.8
  },
  {
    "keyword": "village",
    "weight": 0.7
  },
  {
    "keyword": "people",
    "weight": 0.2
  }
],
...
```

## CamelCase

If you create properties with multiple words (for example `inCountry`) make sure to use CamelCase to divide them. `in-country` or `incountry` will be handled as one word by the contextionary and result in a Weaviate failing to boot or creating a reference to the wrong word.

## Overview

Below an overview of classes for things and actions and how they should be defined.

| Name             | Type     | Should be in contextionary? | Mandatory? | Description |
| ---------------- |:--------:|:--------------------:|:----------:|-------------|
| Class            | `string` | `true`               | `true`     | Noun for Things (i.e., "Place"), verb for action (i.e., "Bought" or "Buys") |
| Class keyword    | `array`  | `true`               | `false`    | An array of descriptions relative to the class. (i.e., the class "Place" might gave: "City" as a keyword) |
| Property         | `string` | `true`               | `true`     | Property of the class. (i.e., "name" for "City") |
| Property keyword | `array`  | `true`               | `false`    | An array of descriptions relative to the class. (i.e., the class "Place" might gave: "City" as a keyword) |
| Value            | `string` | `false`              | `true`     | Value or refererence. |

## Example

A full blown example:

```json
{
  "@context": "http://example.org",
  "classes": [
    {
      "class": "City",
      "description": "This is a test City",
      "keywords": [
        {
          "keyword": "Place",
          "weight": 1
        }
      ],
      "properties": [
        {
          "@dataType": [
            "string"
          ],
          "description": "name of the city.",
          "keywords": [
            {
              "keyword": "keyword",
              "weight": 1
            }
          ],
          "name": "name"
        },
        {
          "@dataType": [
            "int"
          ],
          "description": "Year of establishment.",
          "keywords": [
            {
              "keyword": "keyword",
              "weight": 1
            }
          ],
          "name": "established"
        },
        {
          "@dataType": [
            "number"
          ],
          "description": "Number of inhabitants.",
          "keywords": [
            {
              "keyword": "keyword",
              "weight": 1
            }
          ],
          "name": "inhabitants"
        },
        {
          "@dataType": [
            "Country"
          ],
          "description": "Country that the city is located in.",
          "keywords": [
            {
              "keyword": "keyword",
              "weight": 1
            }
          ],
          "name": "country"
        }
      ]
    },
    {
      "class": "Country",
      "description": "This is a Country",
      "keywords": [
        {
          "keyword": "Place",
          "weight": 1
        }
      ],
      "properties": [
        {
          "@dataType": [
            "string"
          ],
          "description": "Name of the country.",
          "keywords": [
            {
              "keyword": "keyword",
              "weight": 1
            }
          ],
          "name": "name"
        }
      ]
    }
  ],
  "maintainer": "hello@creativesoftwarefdn.org",
  "name": "example.org - Thing Test",
  "type": "thing",
  "version": "1.0.0"
}
```
