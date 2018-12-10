/**
                     __          __                         
_____________  _____/  |_  _____/  |_ ___.__.______   ____  
\____ \_  __ \/  _ \   __\/  _ \   __<   |  |\____ \_/ __ \ 
|  |_> >  | \(  <_> )  | (  <_> )  |  \___  ||  |_> >  ___/ 
|   __/|__|   \____/|__|  \____/|__|  / ____||   __/ \___  >
|__|                                  \/     |__|        \/ 

THIS IS A PROTOTYPE!

Note: you can follow the construction of the Graphql schema by starting underneath: "START CONSTRUCTING THE SERVICE"

*/

// Express for the webserver & graphql
const express = require('express');
const cors = require('cors');
const graphqlHTTP = require('express-graphql');
const demoResolver = require('./demo_resolver/demo_resolver.js');

// file system for reading files
const fs = require('fs');

// define often used GraphQL constants
const {
  GraphQLSchema,
  GraphQLObjectType,
  GraphQLUnionType,
  GraphQLEnumType,
  GraphQLInputObjectType,
  GraphQLInterfaceType,
  GraphQLBoolean,
  GraphQLInt,
  GraphQLFloat,
  GraphQLString,
  GraphQLID,
  GraphQLList,
  GraphQLScalarType,
  GraphQLNonNull
} = require('graphql');

/**
 *  Get descriptions from json file. 
 * NOTE: getDesc(this.name) not possible because this.name refers to keyname of object, and not the key 'name' IN the object (this 'name' is reserved)
 */
const descriptions = JSON.parse(fs.readFileSync('descriptions.json', 'utf8'));
function getDesc(name) {
  return descriptions[name]
}

/**
 * START - ALL RELATED TO INTERNAL FUNCTION
 */

/**
 * Create enum for filter operators
 */
var WhereOperators = new GraphQLEnumType({
  name: "WhereOperatorEnum",
  description: function() {
    return getDesc("WhereOperatorEnum")},
  values: {
    "And": {
      value: "And"
    }, 
    "Or": {
      value: "Or"
    }, 
    "Equal": {
      value: "Equal"
    }, 
    "Not": {
      value: "Not"
    }, 
    "NotEqual": {
      value: "NotEqual"
    }, 
    "GreaterThan": {
      value: "GreaterThan"
    }, 
    "GreaterThanEqual": {
      value: "GreaterThanEqual"
    }, 
    "LessThan": {
      value: "LessThan"
    },
    "LessThanEqual": {
      value: "LessThanEqual"
    },
  }
})

/**
 * Create filter fields for queries
 */
var whereFields = {
  operator: {
    name: "WhereOperator",
    description: function() {
      return getDesc("WhereOperator")},
    type: WhereOperators
  },
  operands: {
    name: "WhereOperands",
    description: function() {
      return getDesc("WhereOperands")},
    type: new GraphQLList(new GraphQLInputObjectType({
      name: "WhereOperandsInpObj",
      description: function() {
        return getDesc("WhereOperandsInpObj")},
      fields: function () {return whereFields}
    }))
  },
  path: { 
    name: "WherePath",
    description: function() {
      return getDesc("WherePath")},
    type: new GraphQLList(GraphQLString) 
  },
  valueInt: { 
    name: "WhereValueInt",
    description: function() {
      return getDesc("WhereValueInt")},
    type: GraphQLInt 
  },
  valueNumber: { 
    name: "WhereValueNumber",
    description: function() {
      return getDesc("WhereValueNumber")},
    type: GraphQLFloat
  },
  valueBoolean: { 
    name: "WhereValueBoolean",
    description: function() {
      return getDesc("WhereValueBoolean")},
    type: GraphQLBoolean
  },
  valueString: { 
    name: "WhereValueString",
    description: function() {
      return getDesc("WhereValueString")},
    type: GraphQLString 
  },
  valueDate: { 
    name: "WhereValueDate",
    description: function() {
      return getDesc("WhereValueDate")},
    type: GraphQLString 
  },
  valueText: { 
    name: "NetworkFetchWherePropertyWhereValueText",
    description: function() {
      return getDesc("WhereValueText")},
    type: GraphQLString 
  }
}



/**
 * Create class enum for filter options
 */
var fuzzyFetchEnum = new GraphQLEnumType({
  name: "fuzzyFetchPropertiesValueTypeEnum",
  description: function() {
    return getDesc("fuzzyFetchPropertiesValueTypeEnum")},
  values: {
    "EQ": {
      value: "EQ"
    }, 
    "NEQ": {
      value: "NEQ"
    }, 
    "PREFIX": {
      value: "PREFIX"
    }, 
    "REGEX": {
      value: "REGEX"
    }, 
    "FUZZY": {
      value: "FUZZY"
    }
  }
})



/**
 * Create class enum for filter options
 */

function createClassEnum(ontologyThings) {

  var enumValues = {}
  var count = 0
  // loop through classes
  ontologyThings.classes.forEach(singleClass => {
    // create enum item
    enumValues[singleClass.class] = {"value": singleClass.class}

    count += 1
  })
  
  var classEnum = new GraphQLEnumType({
    name: 'classEnum',
    description: function() {
      return getDesc("classEnum")},
    values: enumValues,
  });

  return classEnum
}
 
/**
 * Create arguments for the network function
 */
var argsKeywords = new GraphQLInputObjectType({
  name: "argsKeywords",
  description: function() {
    return getDesc("argsKeywords")},
  fields: {
    keyword: {
      name: "WeaviateNetworkKeywordsKeyword",
      description: function() {
        return getDesc("WeaviateNetworkKeywordsKeyword")},
      type: GraphQLString
    },
    weight: {
      name: "WeaviateNetworkKeywordsWeight",
      description: function() {
        return getDesc("WeaviateNetworkKeywordsWeight")},
      type: GraphQLFloat
    }
  }
})


/**
 * create arguments for a search
 */

function createArgs(item, groupBy){
  propsForArgs = {}
  // empty argument
  propsForArgs[item.class] = {}

  // always return first
  propsForArgs[item.class]["first"] = {
    name: "firstFilter",
    type: GraphQLInt,
    description: function() {
      return getDesc("firstFilter")},
  }
  // always return after
  propsForArgs[item.class]["after"] = {
    name: "afterFilter",
    type: GraphQLInt,
    description: function() {
      return getDesc("afterFilter")},
  }

  if(groupBy == true){
    propsForArgs[item.class]["groupBy"] = {
      name: "groupByFilter",
      type: new GraphQLList(GraphQLString),
      description: function() {
        return getDesc("groupByFilter")},
    }
  }
  
  return propsForArgs[item.class] // return the prop with the argument

}


/**
 * Create the subclasses of a Thing or Action in the Local function
 */
function createAggregateSubClasses(ontologyThings, weaviate){
  var subClasses = {};
  // loop through classes
  ontologyThings.classes.forEach(singleClass => {

    //console.log(singleClass.class)

    // create recursive sub classes
    subClasses[singleClass.class] = new GraphQLObjectType({
      name: "Aggregate" + singleClass.class,
      description: singleClass.description,
      fields: function(){
        // declare props that should be returned
        var returnFields = {}

        // add count as field
        returnFields["count"] = {
          name: "Aggregate" + singleClass.class + "Count",
          description: function() {
            return getDesc("AggregateSubClassCount")},
          type: new GraphQLObjectType({
            name: "Aggregate" + singleClass.class + "CountObj",
            description: function() {
              return getDesc("AggregateSubClassCountObj")},
            fields: function(){
              var returnProps = {}
              // loop over properties
              singleClass.properties.forEach(singleClassProperty => {
                singleClassProperty["@dataType"].forEach(singleClassPropertyDatatype => {
                  returnProps[singleClassProperty.name] = {
                    name: "Aggregate" + singleClass.class + singleClassProperty.name,
                    description: singleClassProperty.description,
                    type: GraphQLInt
                  }
                })
              });
              return returnProps
            }
          })
        }

        // checkIfNumericProps
        var numericProps = []
        singleClass.properties.forEach(singleClassProperty => {
          singleClassProperty["@dataType"].forEach(singleClassPropertyDatatype => {
            if(singleClassPropertyDatatype === "int" || singleClassPropertyDatatype === "number") {
              numericProps.push(singleClassProperty)
            }
          })
        })

        if (numericProps.length > 0) {
          // add numeric aggregations as field
          returnFields["minimum"] = {
            name: "Aggregate" + singleClass.class + "Minimum",
            description: function() {
              return getDesc("AggregateSubClassMinimum")},
            type: new GraphQLObjectType({
              name: "Aggregate" + singleClass.class + "MinimumObj",
              description: function() {
                return getDesc("AggregateSubClassMinimumObj")},
              fields: function(){
                var returnProps = {}
                // loop over properties
                singleClass.properties.forEach(numericProps => {
                  numericProps["@dataType"].forEach(singleClassPropertyDatatype => {
                    if(singleClassPropertyDatatype === "int") {
                      returnProps[numericProps.name] = {
                        name: "Aggregate" + singleClass.class + numericProps.name,
                        description: numericProps.description,
                        type: GraphQLInt
                      }
                    } else if(singleClassPropertyDatatype === "number") {
                      returnProps[numericProps.name] = {
                        name: "Aggregate" + singleClass.class + numericProps.name,
                        description: numericProps.description,
                        type: GraphQLFloat
                      }
                    }
                  })
                });
                return returnProps
              }
            })
          },
          returnFields["maximum"] = {
            name: "Aggregate" + singleClass.class + "Maximum",
            description: function() {
              return getDesc("AggregateSubClassMaximum")},
            type: new GraphQLObjectType({
              name: "Aggregate" + singleClass.class + "MaximumObj",
              description: function() {
                return getDesc("AggregateSubClassMaximumObj")},
              fields: function(){
                var returnProps = {}
                // loop over properties
                singleClass.properties.forEach(numericProps => {
                  numericProps["@dataType"].forEach(singleClassPropertyDatatype => {
                    if(singleClassPropertyDatatype === "int") {
                      returnProps[numericProps.name] = {
                        name: "Aggregate" + singleClass.class + numericProps.name,
                        description: numericProps.description,
                        type: GraphQLInt
                      }
                    } else if(singleClassPropertyDatatype === "number") {
                      returnProps[numericProps.name] = {
                        name: "Aggregate" + singleClass.class + numericProps.name,
                        description: numericProps.description,
                        type: GraphQLFloat
                      }
                    }
                  })
                });
                return returnProps
              }
            })
          },
          returnFields["mode"] = {
            name: "Aggregate" + singleClass.class + "Mode",
            description: function() {
              return getDesc("AggregateSubClassMode")},
            type: new GraphQLObjectType({
              name: "Aggregate" + singleClass.class + "ModeObj",
              description: function() {
                return getDesc("AggregateSubClassModeObj")},
              fields: function(){
                var returnProps = {}
                // loop over properties
                singleClass.properties.forEach(numericProps => {
                  numericProps["@dataType"].forEach(singleClassPropertyDatatype => {
                    if(singleClassPropertyDatatype === "int") {
                      returnProps[numericProps.name] = {
                        name: "Aggregate" + singleClass.class + numericProps.name,
                        description: numericProps.description,
                        type: GraphQLInt
                      }
                    } else if(singleClassPropertyDatatype === "number") {
                      returnProps[numericProps.name] = {
                        name: "Aggregate" + singleClass.class + numericProps.name,
                        description: numericProps.description,
                        type: GraphQLFloat
                      }
                    }
                  })
                });
                return returnProps
              }
            })
          },
          returnFields["median"] = {
            name: "Aggregate" + singleClass.class + "Median",
            description: function() {
              return getDesc("AggregateSubClassMedian")},
            type: new GraphQLObjectType({
              name: "Aggregate" + singleClass.class + "MedianObj",
              description: function() {
                return getDesc("AggregateSubClassMedianObj")},
              fields: function(){
                var returnProps = {}
                // loop over properties
                singleClass.properties.forEach(numericProps => {
                  numericProps["@dataType"].forEach(singleClassPropertyDatatype => {
                    if(singleClassPropertyDatatype === "int") {
                      returnProps[numericProps.name] = {
                        name: "Aggregate" + singleClass.class + numericProps.name,
                        description: numericProps.description,
                        type: GraphQLInt
                      }
                    } else if(singleClassPropertyDatatype === "number") {
                      returnProps[numericProps.name] = {
                        name: "Aggregate" + singleClass.class + numericProps.name,
                        description: numericProps.description,
                        type: GraphQLFloat
                      }
                    }
                  })
                });
                return returnProps
              }
            })
          },
          returnFields["sum"] = {
            name: "Aggregate" + singleClass.class + "Sum",
            description: function() {
              return getDesc("AggregateSubClassSum")},
            type: new GraphQLObjectType({
              name: "Aggregate" + singleClass.class + "SumObj",
              description: function() {
                return getDesc("AggregateSubClassSumObj")},
              fields: function(){
                var returnProps = {}
                // loop over properties
                singleClass.properties.forEach(numericProps => {
                  numericProps["@dataType"].forEach(singleClassPropertyDatatype => {
                    if(singleClassPropertyDatatype === "int") {
                      returnProps[numericProps.name] = {
                        name: "Aggregate" + singleClass.class + numericProps.name,
                        description: numericProps.description,
                        type: GraphQLInt
                      }
                    } else if(singleClassPropertyDatatype === "number") {
                      returnProps[numericProps.name] = {
                        name: "Aggregate" + singleClass.class + numericProps.name,
                        description: numericProps.description,
                        type: GraphQLFloat
                      }
                    }
                  })
                });
                return returnProps
              }
            })
          }, 
          returnFields["mean"] = {
            name: "Aggregate" + singleClass.class + "Mean",
            description: function() {
              return getDesc("AggregateSubClassMean")},
            type: new GraphQLObjectType({
              name: "Aggregate" + singleClass.class + "MeanObj",
              description: function() {
                return getDesc("AggregateSubClassMeanObj")},
              fields: function(){
                var returnProps = {}
                // loop over properties
                singleClass.properties.forEach(numericProps => {
                  numericProps["@dataType"].forEach(singleClassPropertyDatatype => {
                    if(singleClassPropertyDatatype === "int") {
                      returnProps[numericProps.name] = {
                        name: "Aggregate" + singleClass.class + numericProps.name,
                        description: numericProps.description,
                        type: GraphQLInt
                      }
                    } else if(singleClassPropertyDatatype === "number") {
                      returnProps[numericProps.name] = {
                        name: "Aggregate" + singleClass.class + numericProps.name,
                        description: numericProps.description,
                        type: GraphQLFloat
                      }
                    }
                  })
                });
                return returnProps
              }
            })
          }
        }

        // add groupedBy as field
        returnFields["groupedBy"] = { // should actually be the property where there is grouped on
          name: "Aggregate" + singleClass.class + "GroupedBy",
          description: function() {
          return getDesc("AggregateSubClassGrouped")},
          type: new GraphQLObjectType({
            name: "Aggregate" + singleClass.class + "GroupedBy",
            description: function() {
              return getDesc("AggregateSubClassGroupedObj")},
            fields: {
              path: {
                name: "AggregateSubClassGroupedPath",
                description: function() {
                  return getDesc("AggregateSubClassGroupedPath")},
                type: new GraphQLList(GraphQLString)
              }, 
              value: {
                name: "AggregateSubClassGroupedValue",
                description: function() {
                  return getDesc("AggregateSubClassGroupedValue")},
                type: GraphQLString
              }
            }
          })
        }


        return returnFields
      }
    });

  });

  return subClasses;
}
 

/**
 * Create the rootclasses of a Thing or Action in the Local function
 */
function createAggregateRootClasses(ontologyThings, subClasses){

  var rootClassesFields = {}

  // loop through classes
  ontologyThings.classes.forEach(singleClass => {
    // create root sub classes
    rootClassesFields[singleClass.class] = {
      name: singleClass.class,
      type: new GraphQLList(subClasses[singleClass.class]),
      description: singleClass.description,
      args: createArgs(singleClass, true),
      resolve(parentValue, args) {
        return demoResolver.aggregateRootClassResolver(parentValue, singleClass.class, args)
      }
    }

  })

  return rootClassesFields

}



/**
 * Create the subclasses of a Thing or Action in the Local function
 */
function createMetaSubClasses(ontologyThings, location='') {

  console.log("------START METASUBCLASSES--------")

  var subClasses = {};
  // loop through classes
  ontologyThings.classes.forEach(singleClass => {

    //console.log(singleClass.class)

    // create recursive sub classes
    subClasses[singleClass.class] = new GraphQLObjectType({
      name: location + "Meta" + singleClass.class,
      description: singleClass.description,
      fields: function(){
        // declare props that should be returned
        var returnProps = {}

        returnProps["meta"] = {
          name: location + "Meta"+ singleClass.class + "Meta",
          description: function() {
            return getDesc("MetaClassMeta")},
          type: new GraphQLObjectType({
            name: location + "Meta" + singleClass.class + "MetaObj",
            description: function() {
              return getDesc("MetaClassMetaObj")},
            fields: {
              count: {
                name: location + "Meta" + singleClass.class + "MetaCount",
                description: function() {
                  return getDesc("MetaClassMetaCount")},
                type: GraphQLInt
              }
            }
          })
        }
        
        // loop over properties
        singleClass.properties.forEach(singleClassProperty => {
          returntypes = []
          standard_fields = {
            type: {
              name: location + "Meta" + singleClass.class + singleClassProperty.name + "Type",
              description: function() {
                return getDesc("MetaClassPropertyType")},
              type: GraphQLString,
            },
            count: {
              name: location + "Meta" + singleClass.class + singleClassProperty.name + "Count",
              description: function() {
                return getDesc("MetaClassPropertyCount")},
              type: GraphQLInt,
            }
          }
          singleClassProperty["@dataType"].forEach(singleClassPropertyDatatype => {

            // if class (start with capital, return Class)
            if(singleClassPropertyDatatype[0] === singleClassPropertyDatatype[0].toUpperCase()){
              returnProps[singleClassProperty.name[0].toUpperCase() + singleClassProperty.name.substring(1)] = {
                name: location + "Meta" + singleClass.class + singleClassProperty.name,
                description: "Meta information about the property \"" + singleClassProperty.name + "\"",
                type: new GraphQLObjectType({
                  name: location + "Meta" + singleClass.class + singleClassProperty.name + "Obj",
                  description: function() {
                    return getDesc("MetaClassPropertyObj")},
                  fields: Object.assign(standard_fields, {
                    pointingTo: {
                      name: location + "Meta" + singleClass.class + singleClassProperty.name + "PointingTo",
                      description: function() {
                        return getDesc("MetaClassPropertyPointingTo")},
                      type: new GraphQLList(GraphQLString)
                    }
                  })
                })
              }
            } else if(singleClassPropertyDatatype === "string" || singleClassPropertyDatatype === "date" || singleClassPropertyDatatype === "text") {
              topOccurrencesType = new GraphQLObjectType({
                name: location + "Meta" + singleClass.class + singleClassProperty.name + "TopOccurrencesObj",
                description: function() {
                  return getDesc("MetaClassPropertyTopOccurrencesObj")},
                fields: {
                  value: {
                    name: location + "Meta" + singleClass.class + singleClassProperty.name + "TopOccurrencesValue",
                    description: function() {
                      return getDesc("MetaClassPropertyTopOccurrencesValue")},
                    type: GraphQLString
                  },
                  occurs: {
                    name: location + "Meta" + singleClass.class + singleClassProperty.name + "TopOccurrencesOccurs",
                    description: function() {
                      return getDesc("MetaClassPropertyTopOccurrencesOccurs")},
                    type: GraphQLInt
                  }
                }
              })
              returnProps[singleClassProperty.name] = {
                name: "Meta" + singleClass.class + singleClassProperty.name,
                description: location + " Meta information about the property \"" + singleClassProperty.name + "\"",
                type: new GraphQLObjectType({
                  name: location + "Meta" + singleClass.class + singleClassProperty.name + "Obj",
                  description: function() {
                    return getDesc("MetaClassPropertyObj")},
                  fields: Object.assign(standard_fields, {
                    topOccurrences: {
                      name: location + "Meta" + singleClass.class + singleClassProperty.name + "TopOccurrences",
                      description: function() {
                        return getDesc("MetaClassPropertyTopOccurrences")},
                      type: new GraphQLList( topOccurrencesType ),
                      args: {
                        first: { 
                          name: "firstFilter",
                          description: function() {
                            return getDesc("firstFilter")},
                          type: GraphQLInt 
                        },
                        after: { 
                          name: "afterFilter",
                          description: function() {
                            return getDesc("afterFilter")},
                          type: GraphQLInt 
                        }
                      },
                      resolve(parentValue, args) {
                        data = parentValue.topOccurrences
                        if (args.after) {
                          data = data.splice(args.after)
                        }
                        if (args.first) {
                          data = data.splice(0, args.first)
                        }
                        return data
                      }
                    }
                  })
                })
              }
            } else if(singleClassPropertyDatatype === "int" || singleClassPropertyDatatype === "number") {
              returnProps[singleClassProperty.name] = {
                name: location + "Meta" + singleClass.class + singleClassProperty.name,
                description: "Meta information about the property \"" + singleClassProperty.name + "\"",
                type: new GraphQLObjectType({
                  name: location + "Meta" + singleClass.class + singleClassProperty.name + "Obj",
                  description: function() {
                    return getDesc("MetaClassPropertyObj")},
                  fields: Object.assign(standard_fields, {
                    lowest: {
                      name: location + "Meta" + singleClass.class + singleClassProperty.name + "Lowest",
                      description: function() {
                        return getDesc("MetaClassPropertyLowest")},
                      type: GraphQLFloat,
                    },
                    highest: {
                      name: location + "Meta" + singleClass.class + singleClassProperty.name + "Highest",
                      description: function() {
                        return getDesc("MetaClassPropertyHighest")},
                      type: GraphQLFloat,
                    },
                    average: {
                      name: location + "Meta" + singleClass.class + singleClassProperty.name + "Average",
                      description: function() {
                        return getDesc("MetaClassPropertyAverage")},
                      type: GraphQLFloat,
                    },
                    sum: {
                      name: location + "Meta" + singleClass.class + singleClassProperty.name + "Sum",
                      description: function() {
                        return getDesc("MetaClassPropertySum")},
                      type: GraphQLFloat,
                    }
                  })
                })
              }
            } else if(singleClassPropertyDatatype === "boolean") {
              returnProps[singleClassProperty.name] = {
                name: location + "Meta" + singleClass.class + singleClassProperty.name,
                description: "Meta information about the property \"" + singleClassProperty.name + "\"",
                type: new GraphQLObjectType({
                  name: location + "Meta" + singleClass.class + singleClassProperty.name + "Obj",
                  description: function() {
                    return getDesc("MetaClassPropertyObj")},
                  fields: Object.assign(standard_fields, {
                    totalTrue: {
                      name: location + "Meta" + singleClass.class + singleClassProperty.name + "TotalTrue",
                      description: function() {
                        return getDesc("MetaClassPropertyTotalTrue")},
                      type: GraphQLInt,
                    },
                    percentageTrue: {
                      name: location + "Meta" + singleClass.class + singleClassProperty.name + "PercentageTrue",
                      description: function() {
                        return getDesc("MetaClassPropertyPercentageTrue")},
                      type: GraphQLFloat,
                    },
                    totalFalse: {
                      name: location + "Meta" + singleClass.class + singleClassProperty.name + "TotalFalse",
                      description: function() {
                        return getDesc("MetaClassPropertyTotalFalse")},
                      type: GraphQLInt,
                    },
                    percentageFalse: {
                      name: location + "Meta" + singleClass.class + singleClassProperty.name + "PercentageFalse",
                      description: function() {
                        return getDesc("MetaClassPropertyPercentageFalse")},
                      type: GraphQLFloat,
                    }
                  })
                })
              }
              // TO DO: CREATE META INFORMATION FOR DATE, NOW THIS IS SAME AS STRING DATATYPE
            // } else if(singleClassPropertyDatatype === "date") {
            //   returnProps[singleClassProperty.name] = {
            //     name: "Meta" + singleClass.class + singleClassProperty.name,
            //     description: singleClassProperty.description,
            //     type: GraphQLString // string since no GraphQL date type exists
            //   }
            } else {
              console.error("I DONT KNOW THIS VALUE! " + singleClassProperty["@dataType"][0])
              returnProps[singleClassProperty.name] = {
                name: location + "Meta" + singleClass.class + singleClassProperty.name,
                description: singleClassProperty.description,
                type: GraphQLString
              }
            }
          })
        });
        return returnProps

       }
    });

  });

  console.log("------DONE METASUBCLASSES--------")

  return subClasses;
}

/**
 * Create the rootclasses of a Thing or Action in the Local function
 */
function createMetaRootClasses(ontologyThings, metaSubClasses){

  console.log("------START METAROOTCLASSES--------")

  var rootClassesFields = {}

  // loop through classes
  ontologyThings.classes.forEach(singleClass => {
    // create root sub classes
    rootClassesFields[singleClass.class] = {
      name: "Meta" + singleClass.class,
      type: metaSubClasses[singleClass.class],
      description: singleClass.description,
      args: createArgs(singleClass),
      resolve(parentValue, args) {
        return demoResolver.metaRootClassResolver(parentValue, singleClass.class, args)
      }
    }

  })

  console.log("------STOP METAROOTCLASSES--------")

  return rootClassesFields

}

/**
 * Create the subclasses of a Thing or Action in the Local function
 */
function createSubClasses(ontologyThings, weaviate){

  console.log("------START SUBCLASSES--------")

  var subClasses = {};
  // loop through classes
  ontologyThings.classes.forEach(singleClass => {

    //console.log(singleClass.class)

    // create recursive sub classes
    subClasses[singleClass.class] = new GraphQLObjectType({
      name: weaviate + singleClass.class,
      description: singleClass.description,
      fields: function(){
        // declare props that should be returned
        var returnProps = {}

        // add uuid to props
        returnProps["uuid"] = {
          name: "SubClassUuid",
          description: function() {
            return getDesc("SubClassUuid")},
          type: GraphQLString
        }

        // loop over properties
        singleClass.properties.forEach(singleClassProperty => {
          returntypes = []
          singleClassProperty["@dataType"].forEach(singleClassPropertyDatatype => {
            // if class (start with capital, return Class)
            if(singleClassPropertyDatatype[0] === singleClassPropertyDatatype[0].toUpperCase()){
              returntypes.push(subClasses[singleClassPropertyDatatype])
            } else if(singleClassPropertyDatatype === "string") {
              returnProps[singleClassProperty.name] = {
                name: weaviate + singleClass.class + singleClassProperty.name,
                description: singleClassProperty.description,
                type: GraphQLString
              }
            } else if(singleClassPropertyDatatype === "int") {
              returnProps[singleClassProperty.name] = {
                name: weaviate + singleClass.class + singleClassProperty.name,
                description: singleClassProperty.description,
                type: GraphQLInt
              }
            } else if(singleClassPropertyDatatype === "number") {
              returnProps[singleClassProperty.name] = {
                name: weaviate + singleClass.class + singleClassProperty.name,
                description: singleClassProperty.description,
                type: GraphQLFloat
              }
            } else if(singleClassPropertyDatatype === "boolean") {
              returnProps[singleClassProperty.name] = {
                name: weaviate + singleClass.class + singleClassProperty.name,
                description: singleClassProperty.description,
                type: GraphQLBoolean
              }
            } else if(singleClassPropertyDatatype === "date") {
              returnProps[singleClassProperty.name] = {
                name: weaviate + singleClass.class + singleClassProperty.name,
                description: singleClassProperty.description,
                type: GraphQLString // string since no GraphQL date type exists
              }
            } else if(singleClassPropertyDatatype === "text") {
              returnProps[singleClassProperty.name] = {
                name: weaviate + singleClass.class + singleClassProperty.name,
                description: singleClassProperty.description,
                type: GraphQLString // text datatype is formatted as string
              }
            } else {
              console.error("I DONT KNOW THIS VALUE! " + singleClassProperty["@dataType"][0])
              returnProps[singleClassProperty.name] = {
                name: weaviate + singleClass.class + singleClassProperty.name,
                description: singleClassProperty.description,
                type: GraphQLString
              }
            }
          })
          if (returntypes.length > 0) {
            returnProps[singleClassProperty.name[0].toUpperCase() + singleClassProperty.name.substring(1)] = {
              name: weaviate + singleClass.class + singleClassProperty.name[0].toUpperCase() + singleClassProperty.name.substring(1),
              description: singleClassProperty.description,
              type: new GraphQLUnionType({
                name: weaviate + singleClass.class + singleClassProperty.name[0].toUpperCase() + singleClassProperty.name.substring(1) + 'Obj', 
                description: singleClassProperty.description,
                types: returntypes,
                resolveType(obj, context, info) {
                  // get returntypes here to return right class types
                  return subClasses[obj.class]
                },
              }),
              //args: createArgs(thing, false),
              resolve(parentValue, obj) {
                console.log("resolve ROOT CLASS " + singleClassProperty.name[0].toUpperCase() + singleClassProperty.name.substring(1))
                if (typeof parentValue[singleClassProperty.name] === "object") {
                  return parentValue[singleClassProperty.name]
                }
                return 
              }
            }
          }
        });
        return returnProps
      }
    });

  });

  console.log("------DONE SUBCLASSES--------")

  return subClasses;
}
 

/**
 * Create the rootclasses of a Thing or Action in the Local function
 */
function createRootClasses(ontologyThings, subClasses){

  console.log("------START ROOTCLASSES--------")

  var rootClassesFields = {}

  // loop through classes
  ontologyThings.classes.forEach(singleClass => {
    // create root sub classes
    rootClassesFields[singleClass.class] = {
      name: singleClass.class,
      type: new GraphQLList(subClasses[singleClass.class]),
      description: singleClass.description,
      args: createArgs(singleClass),
      resolve(parentValue, args) {
        return demoResolver.rootClassResolver(parentValue, singleClass.class, args)
      }
    }

  })

  console.log("------STOP ROOTCLASSES--------")

  return rootClassesFields

}


/**
 * Merge ontologies because both actions and things can refer to eachother
 */
function mergeOntologies(a, b){
  var classCount = [];
 
  var classes = {}
  classes["classes"] = []

  a.classes.forEach(singleClassA => {
    classCount.push(singleClassA.class)
    classes["classes"].push(singleClassA)
  })

  b.classes.forEach(singleClassB => {
    classes["classes"].push(singleClassB)
  })

  console.log("------MERGED ONTOLOGIES--------")
  return classes
}


/**
 * END - ALL RELATED TO INTERNAL
 */


/**
 * START - ALL RELATED TO NETWORK
 */


function getWeaviateNetworkGetFields(weaviate) {
  var thingsFile = './network/' + weaviate + '/things_schema.json';
  var actionsFile = './network/' + weaviate + '/actions_schema.json';

  //let ontologyThings = require(thingsFile);
  //let ontologyActions = require(actionsFile);

  let ontologyThings = fs.readFileSync(thingsFile, {encoding:'utf8'});
  let ontologyActions = fs.readFileSync(actionsFile, {encoding:'utf8'});

  // merge
  classes = mergeOntologies(JSON.parse(ontologyThings), JSON.parse(ontologyActions))
  var localSubClasses = createSubClasses(classes, weaviate);
  var rootClassesNetworkThingsFields = createRootClasses(JSON.parse(ontologyThings), localSubClasses);
  var rootClassesNetworkActionsFields = createRootClasses(JSON.parse(ontologyActions), localSubClasses);

  fields = {
    Things: {
      name: "WeaviateNetworkGet" + weaviate + "Things",
      description: function() {
        return getDesc("WeaviateNetworkGetThings")},
      type: new GraphQLObjectType({
        name: "WeaviateNetworkGet" + weaviate + "ThingsObj",
        description: function() {
          return getDesc("WeaviateNetworkGetThingsObj")},
        fields: rootClassesNetworkThingsFields
      }),
      resolve(parentValue) {
        console.log("resolve WeaviateNetworkGet" + weaviate + "Things")
        return parentValue.Things // resolve with empty array
      },
    },
    Actions: {
      name: "WeaviateNetworkGet" + weaviate + "Actions",
      description: function() {
        return getDesc("WeaviateNetworkGetActions")},
      type: new GraphQLObjectType({
        name: "WeaviateNetworkGet" + weaviate + "ActionsObj",
        description: function() {
          return getDesc("WeaviateNetworkGetActionsObj")},
        fields: rootClassesNetworkActionsFields
      }),
      resolve(parentValue) {
        console.log("resolve WeaviateNetworkGet" + weaviate + "Actions")
        return parentValue.Actions // resolve with empty array
      }
    }
  }
return fields

}

function getWeaviateNetworkGetMetaFields(weaviate) {
  var thingsFile = './network/' + weaviate + '/things_schema.json';
  var actionsFile = './network/' + weaviate + '/actions_schema.json';

  let ontologyThings = fs.readFileSync(thingsFile, {encoding:'utf8'});
  let ontologyActions = fs.readFileSync(actionsFile, {encoding:'utf8'});

  // merge
  classes = mergeOntologies(JSON.parse(ontologyThings), JSON.parse(ontologyActions))
  var metaSubClasses = createMetaSubClasses(classes, weaviate);
  var metaRootClassesNetworkThingsFields = createMetaRootClasses(JSON.parse(ontologyThings), metaSubClasses);
  var metaRootClassesNetworkActionsFields = createMetaRootClasses(JSON.parse(ontologyActions), metaSubClasses);

  fields = {
    Things: {
      name: "WeaviateNetworkGetMeta" + weaviate + "Things",
      description: function() {
        return getDesc("WeaviateNetworkGetMetaThings")},
      type: new GraphQLObjectType({
        name: "WeaviateNetworkGetMeta" + weaviate + "ThingsObj",
        description: function() {
          return getDesc("WeaviateNetworkGetMetaThingsObj")},
        fields: metaRootClassesNetworkThingsFields
      }),
      resolve(parentValue) {
        console.log("resolve WeaviateNetworkGetMeta" + weaviate + "Things")
        return parentValue.Things // resolve with empty array
      },
    },
    Actions: {
      name: "WeaviateNetworkGetMeta" + weaviate + "Actions",
      description: function() {
        return getDesc("WeaviateNetworkGetMetaActions")},
      type: new GraphQLObjectType({
        name: "WeaviateNetworkGetMeta" + weaviate + "ActionsObj",
        description: function() {
          return getDesc("WeaviateNetworkGetMetaActionsObj")},
        fields: metaRootClassesNetworkActionsFields
      }),
      resolve(parentValue) {
        console.log("resolve WeaviateNetworkGetMeta" + weaviate + "Actions")
        return parentValue.Actions // resolve with empty array
      }
    }
  }
return fields

}

function createNetworkWeaviateGetFields() {
  console.log("------START NETWORKWEAVIATEGETFIELDS--------")
  var networkFields = {}

  function getDirectories(path) {
    return fs.readdirSync(path).filter(function (file) {
      return fs.statSync(path+'/'+file).isDirectory();
    });
  }
  var weaviates = getDirectories("./network");

  weaviates.forEach(weaviate => {
    weaviate = weaviate[0].toUpperCase() + weaviate.substring(1);
    networkFields[weaviate] = {
      name: "WeaviateNetworkGet" + weaviate,
      description: "Object field for weaviate " + weaviate + " in the network.",
      type: new GraphQLObjectType({
        name: "WeaviateNetworkGet" + weaviate + "Obj",
        description: "Objects for the what to Get from the weaviate " + weaviate + " in the network.",
        fields: getWeaviateNetworkGetFields(weaviate)
      }),
      resolve(parentValue){
        console.log("resolve WeaviateNetworkGet" + weaviate)
        return parentValue[weaviate] // resolve with empty array
      }
    }
  })

  console.log("------STOP NETWORKWEAVIATEGETFIELDS--------")
  return networkFields
}

function createNetworkWeaviateGetMetaFields() {
  console.log("------START NETWORKWEAVIATEGETMETAFIELDS--------")
  var networkFields = {}

  function getDirectories(path) {
    return fs.readdirSync(path).filter(function (file) {
      return fs.statSync(path+'/'+file).isDirectory();
    });
  }
  var weaviates = getDirectories("./network");

  weaviates.forEach(weaviate => {
    weaviate = weaviate[0].toUpperCase() + weaviate.substring(1)
    networkFields[weaviate] = {
      name: "WeaviateNetworkGetMeta" + weaviate,
      description: "Object field for weaviate " + weaviate + " in the network.",
      type: new GraphQLObjectType({
        name: "WeaviateNetworkGetMeta" + weaviate + "Obj",
        description: "Objects for the what to Get Meta from the weaviate " + weaviate + " in the network.",
        fields: getWeaviateNetworkGetMetaFields(weaviate)
      }),
      resolve(parentValue){
        console.log("resolve WeaviateNetworkGetMeta" + weaviate)
        return parentValue[weaviate] // resolve with empty array
      }
    }
  })

  console.log("------STOP NETWORKWEAVIATEGETMETAFIELDS--------")
  return networkFields
}

var NetworkIntrospectThingsActionsFields = {
  weaviate: {
    name: "WeaviateNetworkIntrospectWeaviate",
    description: function() {
      return getDesc("WeaviateNetworkIntrospectWeaviate")},
    type: GraphQLString
  },
  className: {
    name: "WeaviateNetworkIntrospectClassName",
    description: function() {
      return getDesc("WeaviateNetworkIntrospectClassName")},
    type: GraphQLString
  },
  certainty: {
    name: "WeaviateNetworkIntrospectCertainty",
    description: function() {
      return getDesc("WeaviateNetworkIntrospectCertainty")},
    type: GraphQLFloat
  },
  properties: {
    name: "WeaviateNetworkIntrospectProperties",
    description: function() {
      return getDesc("WeaviateNetworkIntrospectProperties")},
    type: new GraphQLList(new GraphQLObjectType({
      name: "WeaviateNetworkIntrospectPropertiesObj",
      description: function() {
        return getDesc("WeaviateNetworkIntrospectPropertiesObj")},
      fields: {
        propertyName: {
          name: "WeaviateNetworkIntrospectPropertiesPropertyName",
          description: function() {
            return getDesc("WeaviateNetworkIntrospectPropertiesPropertyName")},
          type: GraphQLString
        },
        certainty: {
          name: "WeaviateNetworkIntrospectPropertiesCertainty",
          description: function() {
            return getDesc("WeaviateNetworkIntrospectPropertiesCertainty")},
          type: GraphQLFloat
        }
      }
    }))
  }
}

var NetworkIntrospectBeaconFields = {
  weaviate: {
    name: "WeaviateNetworkIntrospectBeaconWeaviate",
    description: function() {
      return getDesc("WeaviateNetworkIntrospectWeaviate")},
    type: GraphQLString
  },
  className: {
    name: "WeaviateNetworkIntrospectBeaconClassName",
    description: function() {
      return getDesc("WeaviateNetworkIntrospectClassName")},
    type: GraphQLString
  },
  properties: {
    name: "WeaviateNetworkIntrospectBeaconProperties",
    description: function() {
      return getDesc("WeaviateNetworkIntrospectProperties")},
    type: new GraphQLList(new GraphQLObjectType({
      name: "WeaviateNetworkIntrospectBeaconPropertiesObj",
      description: function() {
        return getDesc("WeaviateNetworkIntrospectBeaconPropertiesObj")},
      fields: {
        propertyName: {
          name: "WeaviateNetworkIntrospectPropertiesBeaconPropertyName",
          description: function() {
            return getDesc("WeaviateNetworkIntrospectPropertiesPropertyName")},
          type: GraphQLString
        }
      }
    }))
  }
}

var NetworkFetchWherePropertyFilterFields = {
  name: {
    name: "NetworkFetchWherePropertyWhereName",
    description: function() {
      return getDesc("NetworkFetchWherePropertyWhereName")},
    type: GraphQLString,
  }, 
  keywords: {
    name: "NetworkFetchWherePropertyWhereKeywords",
    description: function() {
      return getDesc("NetworkFetchWherePropertyWhereKeywords")},
    type: new GraphQLList(new GraphQLInputObjectType({
      name: "NetworkFetchWherePropertyWhereKeywordsInpObj",
      description: function() {
        return getDesc("NetworkFetchWherePropertyWhereKeywordsInpObj")},
      fields: {
        value: {
          name: "NetworkFetchWherePropertyWhereKeywordsValue",
          description: function() {
            return getDesc("NetworkFetchWherePropertyWhereKeywordsValue")},
          type: GraphQLString,
        },
        weight: {
          name: "NetworkFetchWherePropertyWhereKeywordsWeight",
          description: function() {
            return getDesc("NetworkFetchWherePropertyWhereKeywordsWeight")},
          type: GraphQLFloat,
        }
      }
    }))
  }, 
  certainty: {
    name: "NetworkFetchWherePropertyWhereCertainty",
    description: function() {
      return getDesc("NetworkFetchWherePropertyWhereCertainty")},
    type: GraphQLFloat,
  },
  operator: {
    name: "NetworkFetchWherePropertyWhereOperator",
    description: function() {
      return getDesc("NetworkFetchWherePropertyWhereOperator")},
    type: WhereOperators
  },
  valueInt: { 
    name: "NetworkFetchWherePropertyWhereValueInt",
    description: function() {
      return getDesc("WhereValueInt")},
    type: GraphQLInt 
  },
  valueNumber: { 
    name: "NetworkFetchWherePropertyWhereValueNumber",
    description: function() {
      return getDesc("WhereValueNumber")},
    type: GraphQLFloat
  },
  valueBoolean: { 
    name: "NetworkFetchWherePropertyWhereValueBoolean",
    description: function() {
      return getDesc("WhereValueBoolean")},
    type: GraphQLBoolean
  },
  valueString: { 
    name: "NetworkFetchWherePropertyWhereValueString",
    description: function() {
      return getDesc("WhereValueString")},
    type: GraphQLString 
  },
  valueDate: { 
    name: "NetworkFetchWherePropertyWhereValueDate",
    description: function() {
      return getDesc("WhereValueDate")},
    type: GraphQLString 
  },
  valueText: { 
    name: "NetworkFetchWherePropertyWhereValueText",
    description: function() {
      return getDesc("WhereValueText")},
    type: GraphQLString 
  }
}

/**
 * Create class and property filter options for network fetch 
 */
var NetworkIntrospectWhereClassAndPropertyFilterFields = {
  name: {
    name: "WeaviateNetworkWhereName",
    description: function() {
      return getDesc("WeaviateNetworkWhereName")},
    type: GraphQLString,
  }, 
  keywords: {
    name: "WeaviateNetworkWhereNameKeywords",
    description: function() {
      return getDesc("WeaviateNetworkWhereNameKeywords")},
    type: new GraphQLList(new GraphQLInputObjectType({
      name: "WeaviateNetworkWhereNameKeywordsInpObj",
      description: function() {
        return getDesc("WeaviateNetworkWhereNameKeywordsInpObj")},
      fields: {
        value: {
          name: "WeaviateNetworkWhereNameKeywordsValue",
          description: function() {
            return getDesc("WeaviateNetworkWhereNameKeywordsValue")},
          type: GraphQLString,
        },
        weight: {
          name: "WeaviateNetworkWhereNameKeywordsWeight",
          description: function() {
            return getDesc("WeaviateNetworkWhereNameKeywordsWeight")},
          type: GraphQLFloat,
        }
      }
    }))
  }, 
  certainty: {
    name: "WeaviateNetworkWhereCertainty",
    description: function() {
      return getDesc("WeaviateNetworkWhereCertainty")},
    type: GraphQLFloat,
  }, 
  first: {
    name: "WeaviateNetworkWhereFirst",
    description: function() {
      return getDesc("WeaviateNetworkWhereFirst")},
    type: GraphQLInt,
  }
}


/**
 * Create filter options for network fetch 
 */
var NetworkIntrospectWhereFilterFields = {
  where: { 
    name: "WeaviateNetworkIntrospectWhere",
    description: function() {
      return getDesc("WeaviateNetworkIntrospectWhere")},
    type: new GraphQLNonNull( new GraphQLList(new GraphQLInputObjectType({
      name: "WeaviateNetworkIntrospectWhereInpObj",
      description: function() {
        return getDesc("WeaviateNetworkIntrospectWhereInpObj")},
      fields: {
        class: {
          name: "WeaviateNetworkIntrospectWhereClass",
          description: function() {
            return getDesc("WeaviateNetworkIntrospectWhereClass")},
          type: new GraphQLList(new GraphQLInputObjectType({
            name: "WeaviateNetworkIntrospectWhereClassObj",
            description: function() {
              return getDesc("WeaviateNetworkIntrospectWhereClassObj")},
            fields: NetworkIntrospectWhereClassAndPropertyFilterFields
          }))
        },
        properties: {
          name: "WeaviateNetworkIntrospectWhereProperties",
          description: function() {
            return getDesc("WeaviateNetworkIntrospectWhereProperties")},
          type: new GraphQLList(new GraphQLInputObjectType({
            name: "WeaviateNetworkIntrospectWherePropertiesObj",
            description: function() {
              return getDesc("WeaviateNetworkIntrospectWherePropertiesObj")},
            fields: NetworkIntrospectWhereClassAndPropertyFilterFields
          }))
        }
      }
    }))) //Needs to be in contextionary, weight = always 1.0
  }
}


var NetworkFetchFilterFields = {
  where: { 
    name: "WeaviateNetworkFetchWhere",
    description: function() {
      return getDesc("NetworkFetchWhere")},
    type: new GraphQLNonNull( new GraphQLInputObjectType({
      name: "WeaviateNetworkFetchWhereInpObj",
      description: function() {
        return getDesc("NetworkFetchWhereInpObj")},
      fields: {
        class: {
          name: "WeaviateNetworkFetchWhereInpObjClass",
          description: function() {
            return getDesc("NetworkFetchWhereInpObjClass")},
          type: new GraphQLList(new GraphQLInputObjectType({
            name: "WeaviateNetworkFetchWhereInpObjClassInpObj",
            description: function() {
              return getDesc("NetworkFetchWhereInpObjClassInpObj")},
            fields: NetworkIntrospectWhereClassAndPropertyFilterFields
          }))
        },
        properties: {
          name: "WeaviateNetworkFetchWhereInpObjProperties",
          description: function() {
            return getDesc("NetworkFetchWhereInpObjProperties")},
          type: new GraphQLList(new GraphQLInputObjectType({
            name: "WeaviateNetworkFetchWhereInpObjProperties",
            description: function() {
              return getDesc("NetworkFetchWhereInpObjProperties")},
            fields: NetworkFetchWherePropertyFilterFields
          }))
        },
        first: {
          name: "WeaviateNetworkFetchWhereInpObjFirst",
          description: function() {
            return getDesc("NetworkFetchWhereInpObjFirst")},
          type: GraphQLInt,
        }
      }
    })) //Needs to be in contextionary, weight = always 1.0
  }
}



/**
 * END - ALL RELATED TO NETWORK
 */

/**
 * START CONSTRUCTING THE SERVICE
 */
//var demo_schema_things = "demo_schemas/things_schema.json";
//var demo_schema_actions = "demo_schemas/actions_schema.json";

// check if the test schemas should be used
var runArguments = process.argv.slice(2);
if(runArguments[0] == "test_schema"){
  console.log("running the test schemas used for Weaviate testing in Go")
  var demo_schema_things = "../../test/schema/test-thing-schema.json";
  var demo_schema_actions = "../../test/schema/test-action-schema.json";
} else if(runArguments[0] == "demo_schema"){
  console.log("running the demo schemas used in use case and documentation examples")
  var demo_schema_things = "demo_schemas/things_schema.json";
  var demo_schema_actions = "demo_schemas/actions_schema.json";
}

fs.readFile(demo_schema_things, 'utf8', function(err, ontologyThings) { // read things ontology
  fs.readFile(demo_schema_actions, 'utf8', function(err, ontologyActions) { // read actions ontology

    // merge
    classes = mergeOntologies(JSON.parse(ontologyThings), JSON.parse(ontologyActions))

    // create GraphQL fields for words in contextionary
    // var contextionaryWords = createContextionaryFields(nouns);
  
    // create the root and sub classes based on the Weaviate schemas
    var localSubClasses = createSubClasses(classes, "");
    var rootClassesThingsFields = createRootClasses(JSON.parse(ontologyThings), localSubClasses);
    var rootClassesActionsFields = createRootClasses(JSON.parse(ontologyActions), localSubClasses);
    var classesEnum = createClassEnum(classes);
    // var PinPointField = createPinPointField(classes);
    var metaSubClasses = createMetaSubClasses(classes)
    var metaRootClassesThingsFields = createMetaRootClasses(JSON.parse(ontologyThings), metaSubClasses);
    var metaRootClassesActionsFields = createMetaRootClasses(JSON.parse(ontologyActions), metaSubClasses);

    var aggregateSubClasses = createAggregateSubClasses(classes)
    var aggregateRootClassesThingsFields = createAggregateRootClasses(JSON.parse(ontologyThings), aggregateSubClasses);
    var aggregateRootClassesActionsFields = createAggregateRootClasses(JSON.parse(ontologyActions), aggregateSubClasses);

    var WeaviateNetworkGetFields = createNetworkWeaviateGetFields()
    var WeaviateNetworkGetMetaFields = createNetworkWeaviateGetMetaFields()

    // This is the root 
    var Weaviate = new GraphQLObjectType({
      name: 'WeaviateObj',
      description: function() {
        return getDesc("WeaviateObj")},
      fields: {
        Local: {
          name: "WeaviateLocal",
          description: function() {
            return getDesc("WeaviateLocal")},
          resolve() {
            console.log("resolve WeaviateLocal")
            return [{}] // resolve with empty array
          },
          type: new GraphQLObjectType({
            name: "WeaviateLocalObj",
            description: function() {
              return getDesc("WeaviateLocalObj")},
            resolve() {
              console.log("resolve WeaviateLocalObj")
              return [{}] // resolve with empty array
            },
            fields: {
              Get: {
                name: "WeaviateLocalGet",
                description: function() {
                  return getDesc("WeaviateLocalGet")},
                args: {
                  where: { 
                    name: "WeaviateLocalGetWhere",
                    description: function() {
                      return getDesc("WeaviateLocalGetWhere")},
                    type: new GraphQLInputObjectType({
                      name: "WeaviateLocalGetWhereInpObj",
                      description: function() {
                        return getDesc("WeaviateLocalGetWhereInpObj")},
                      fields: whereFields
                    }) 
                  }
                },
                type: new GraphQLObjectType({
                  name: "WeaviateLocalGetObj",
                  description: function() {
                    return getDesc("WeaviateLocalGetObj")},
                  fields: {
                    Things: {
                      name: "WeaviateLocalGetThings",
                      description: function() {
                        return getDesc("WeaviateLocalGetThings")},
                      type: new GraphQLObjectType({
                        name: "WeaviateLocalGetThingsObj",
                        description: function() {
                          return getDesc("WeaviateLocalGetThingsObj")},
                        fields: rootClassesThingsFields
                      }),
                      resolve(parentValue) {
                        console.log("resolve WeaviateLocalGetThings")
                        return parentValue.Things // resolve with empty array
                      },
                    },
                    Actions: {
                      name: "WeaviateLocalGetActions",
                      description: function() {
                        return getDesc("WeaviateLocalGetActions")},
                      type: new GraphQLObjectType({
                        name: "WeaviateLocalGetActionsObj",
                        description: function() {
                          return getDesc("WeaviateLocalGetActionsObj")},
                        fields: rootClassesActionsFields
                      }),
                      resolve(parentValue) {
                        console.log("resolve WeaviateLocalGetActions")
                        return parentValue.Actions // resolve with empty array
                      }
                    }
                  }
                }),
                resolve(parentValue, args) {
                  console.log("resolve WeaviateLocalGet")
                  result = demoResolver.resolveGet(args.where)
                  if (result != 'error') {
                    return result
                  }
                  else {throw new Error('Text values cannot be filtered because they are not indexed.')}
                },
              },
              GetMeta: {
                name: "WeaviateLocalGetMeta",
                description: function() {
                  return getDesc("WeaviateLocalGetMeta")},
                args: {
                  where: { 
                    name: "WeaviateLocalGetMetaWhere",
                    description: function() {
                      return getDesc("WeaviateLocalGetMetaWhere")},
                    type: new GraphQLInputObjectType({
                      name: "WeaviateLocalGetMetaWhereInpObj",
                      description: function() {
                        return getDesc("WeaviateLocalGetMetaWhereInpObj")},
                      fields: whereFields
                    }) 
                }
                },
                type: new GraphQLObjectType({
                  name: "WeaviateLocalGetMetaObj",
                  description: function() {
                    return getDesc("WeaviateLocalGetMetaObj")},
                  fields: {
                    Things: {
                      name: "WeaviateLocalGetMetaThings",
                      description: function() {
                        return getDesc("WeaviateLocalGetMetaThings")},
                      type: new GraphQLObjectType({
                        name: "WeaviateLocalGetMetaThingsObj",
                        description: function() {
                          return getDesc("WeaviateLocalGetMetaThingsObj")},
                        fields: metaRootClassesThingsFields
                      }),
                      resolve(parentValue, args) {
                        console.log("resolve WeaviateLocalGetMetaThings")
                        return parentValue.Things // resolve with empty array
                      }
                    }, 
                    Actions: {
                      name: "WeaviateLocalGetMetaActions",
                      description: function() {
                        return getDesc("WeaviateLocalGetMetaActions")},
                      type: new GraphQLObjectType({
                        name: "WeaviateLocalGetMetaActionsObj",
                        description: function() {
                          return getDesc("WeaviateLocalGetMetaActionsObj")},
                        fields: metaRootClassesActionsFields
                      }),
                      resolve(parentValue, args) {
                        console.log("resolve WeaviateLocalGetMetaActions")
                        return parentValue.Actions // resolve with empty array
                      }
                    }
                  },
                }),
                resolve(parentValue, args) {
                  console.log("resolve WeaviateLocalGetMeta")
                  result = demoResolver.resolveGet(args.where)
                  if (result != 'error') {
                    return result
                  }
                  else {throw new Error('Text values cannot be filtered because they are not indexed.')}
                },
              },
              Aggregate: {
                name: "WeaviateLocalAggregate",
                description: function() {
                  return getDesc("WeaviateLocalAggregate")},
                args: {
                  where: { 
                    name: "WeaviateLocalAggregateWhere",
                    description: function() {
                      return getDesc("WeaviateLocalAggregateWhere")},
                    type: new GraphQLInputObjectType({
                      name: "WeaviateLocalAggregateWhereInpObj",
                      description: function() {
                        return getDesc("WeaviateLocalAggregateWhereInpObj")},
                      fields: whereFields
                    }) 
                  }
                },
                type: new GraphQLObjectType({
                  name: "WeaviateLocalAggregateObj",
                  description: function() {
                    return getDesc("WeaviateLocalAggregateObj")},
                  fields: {
                    Things: {
                      name: "WeaviateLocalAggregateThings",
                      description: function() {
                        return getDesc("WeaviateLocalAggregateThings")},
                      type: new GraphQLObjectType({
                        name: "WeaviateLocalAggregateThingsObj",
                        description: function() {
                          return getDesc("WeaviateLocalAggregateThingsObj")},
                        fields: aggregateRootClassesThingsFields
                      }),
                      resolve(parentValue) {
                        console.log("resolve WeaviateLocalAggregateThings")
                        return parentValue.Things // resolve with empty array
                      },
                    },
                    Actions: {
                      name: "WeaviateLocalAggregateActions",
                      description: function() {
                        return getDesc("WeaviateLocalAggregateActions")},
                      type: new GraphQLObjectType({
                        name: "WeaviateLocalAggregateActionsObj",
                        description: function() {
                          return getDesc("WeaviateLocalAggregateActionsObj")},
                        fields: aggregateRootClassesActionsFields
                      }),
                      resolve(parentValue) {
                        console.log("resolve WeaviateLocalAggregateActions")
                        return parentValue.Actions // resolve with empty array
                      }
                    }
                  }
                }),
                resolve(parentValue, args) {
                  console.log("resolve WeaviateLocalAggregate")
                  result = demoResolver.resolveGet(args.where)
                  if (result != 'error') {
                    return result
                  }
                  else {throw new Error('Text values cannot be filtered because they are not indexed.')}
                },
              }
            }
          })
        },
        Network: {
          name: "WeaviateNetwork",
          description: function() {
            return getDesc("WeaviateNetwork")},
          args: {
            networkTimeout: { 
              name: "WeaviateNetworkNetworkTimeout",
              description: function() {
                return getDesc("WeaviateNetworkNetworkTimeout")},
              type: GraphQLInt
            },
            // network: {
            //   name: "WeaviateNetworkNetworkNetwork",
            //   description: function() {
            //     return getDesc("WeaviateNetworkNetworkNetwork")},
            //   type: GraphQLString
            // }
          },
          resolve() {
            console.log("resolve WeaviateNetwork")
            return [{}] // resolve with empty array
          },
          type: new GraphQLObjectType({
            name: "WeaviateNetworkObj",
            description: function() {
              return getDesc("WeaviateNetworkObj")},
            fields: {
              Get: {
                name: "WeaviateNetworkGet",
                description: function() {
                  return getDesc("WeaviateNetworkGet")},
                args: {
                  where: { 
                    name: "WeaviateNetworkGetWhere",
                    description: function() {
                      return getDesc("WeaviateNetworkGetWhere")},
                    type: new GraphQLInputObjectType({
                      name: "WeaviateNetworkGetWhereInpObj",
                      description: function() {
                        return getDesc("WeaviateNetworkGetWhereInpObj")},
                      fields: whereFields
                    }) 
                  }
                },
                type: new GraphQLObjectType({
                  name: "WeaviateNetworkGetObj",
                  description: function() {
                    return getDesc("WeaviateNetworkGetObj")},
                  fields: WeaviateNetworkGetFields
                }),
                resolve(parentValue, args) {
                  console.log("resolve WeaviateNetworkGet")
                  result = demoResolver.resolveNetworkGet(args.where)
                  if (result != 'error') {
                    return result
                  }
                  else {throw new Error('Text values cannot be filtered because they are not indexed.')}
                },
              },
              Fetch: {
                name: "WeaviateNetworkFetch",
                description: function() {
                  return getDesc("WeaviateNetworkFetch")},
                type: new GraphQLObjectType({
                  name: "WeaviateNetworkFetchObj",
                  description: function() {
                    return getDesc("WeaviateNetworkFetchObj")},
                  fields: {
                    Fuzzy: {
                      name: "WeaviateNetworkFetchFuzzy",
                      description: function() {
                        return getDesc("WeaviateNetworkFetchFuzzy")},
                      args: {
                        value: { 
                          name: "WeaviateNetworkFetchFuzzyArgValue",
                          description: function() {
                            return getDesc("WeaviateNetworkFetchFuzzyArgValue")},
                          type: new GraphQLNonNull(GraphQLString)
                        },
                        certainty: { 
                          name: "WeaviateNetworkFetchFuzzyArgCertainty",
                          description: function() {
                            return getDesc("WeaviateNetworkFetchFuzzyArgCertainty")},
                          type: new GraphQLNonNull(GraphQLFloat)
                          }
                        },
                      type: new GraphQLList(new GraphQLObjectType({
                        name: "WeaviateNetworkFetchFuzzyObj",
                        description: function() {
                          return getDesc("WeaviateNetworkFetchFuzzyObj")},
                        fields: {
                          beacon: { // The beacon to do a convertedfetch
                            name: "WeaviateNetworkFetchFuzzyBeacon",
                            description: function() {
                              return getDesc("WeaviateNetworkFetchFuzzyBeacon")},
                            type: GraphQLString
                          }, 
                          certainty: { //  What is the certainty to the original request?
                            name: "WeaviateNetworkFetchFuzzyCertainty",
                            description: function() {
                              return getDesc("WeaviateNetworkFetchFuzzyCertainty")},
                            type: GraphQLFloat
                          }
                        }
                      })),
                      resolve(parentValue, args) {
                        console.log("resolve WeaviateNetworkFetchFuzzy")
                        return demoResolver.resolveNetworkFetchFuzzy(args)
                      }
                    },
                    Things: {
                      name: "WeaviateNetworkFetchThings",
                      description: function() {
                        return getDesc("WeaviateNetworkFetchThings")},
                      args: NetworkFetchFilterFields,
                      type: new GraphQLList(new GraphQLObjectType({
                        name: "WeaviateNetworkFetchThingsObj",
                        description: function() {
                          return getDesc("WeaviateNetworkFetchThingsObj")},
                        fields: {
                          beacon: { // The beacon to do a convertedfetch
                            name: "WeaviateNetworkFetchThingsBeacon",
                            description: function() {
                              return getDesc("WeaviateNetworkFetchThingsBeacon")},
                            type: GraphQLString
                          }, 
                          certainty: { //  What is the certainty to the original request?
                            name: "WeaviateNetworkFetchThingsCertainty",
                            description: function() {
                              return getDesc("WeaviateNetworkFetchThingsCertainty")},
                            type: GraphQLFloat
                          }
                        }
                      })),
                      resolve(parentValue, args) {
                        console.log("resolve WeaviateNetworkFetchThings")
                        return demoResolver.resolveNetworkFetch(args)
                      }
                    },
                    Actions: {
                      name: "WeaviateNetworkFetchActions",
                      description: function() {
                        return getDesc("WeaviateNetworkFetchActions")},
                      args: NetworkFetchFilterFields,
                      type: new GraphQLList(new GraphQLObjectType({
                        name: "WeaviateNetworkFetchActionsObj",
                        description: function() {
                          return getDesc("WeaviateNetworkFetchActionsObj")},
                        fields: {
                          beacon: {
                            name: "WeaviateNetworkFetchActionsBeacon",
                            description: function() {
                              return getDesc("WeaviateNetworkFetchActionsBeacon")},
                            type: GraphQLString,
                            resolve(parentValue, args) {
                              console.log("resolve WeaviateNetworkFetchActionsBeacon")
                              return [{}] // resolve with empty array
                            }
                          }, 
                          certainty: {
                            name: "WeaviateNetworkFetchActionsCertainty",
                            description: function() {
                              return getDesc("WeaviateNetworkFetchActionsCertainty")},
                            type: GraphQLFloat, // should be enum of type (id est, string, int, cref etc)
                            resolve(parentValue, args) {
                              console.log("resolve WeaviateNetworkFetchActionsCertainty")
                              return [{}] // resolve with empty array
                            }
                          }
                        }
                      })),
                      resolve(parentValue, args) {
                        console.log("resolve WeaviateNetworkFetchActions")
                        return demoResolver.resolveNetworkFetch(args)
                      }
                    },
                  }
                }),
                resolve() {
                  console.log("resolve WeaviateNetworkFetch")
                  return [{}] // resolve with empty array
                },
              },
              Introspect: {
                name: "WeaviateNetworkIntrospect",
                description: function() {
                  return getDesc("WeaviateNetworkIntrospect")},
                type: new GraphQLObjectType({
                  name: "WeaviateNetworkIntrospectObj",
                  description: function() {
                    return getDesc("WeaviateNetworkIntrospectObj")},
                  fields: {
                    Things: {
                      name: "WeaviateNetworkIntrospectThings",
                      description: function() {
                        return getDesc("WeaviateNetworkIntrospectThings")},
                      args: NetworkIntrospectWhereFilterFields,
                      type: new GraphQLList(new GraphQLObjectType({
                        name: "WeaviateNetworkIntrospectThingsObj",
                        description: function() {
                          return getDesc("WeaviateNetworkIntrospectThingsObj")},
                        fields: NetworkIntrospectThingsActionsFields
                      })),
                      resolve(parentValue, args) {
                        console.log("resolve WeaviateNetworkIntrospectThings")
                        return demoResolver.resolveNetworkIntrospect(args) // resolve with empty array
                      }
                    },
                    Actions: {
                      name: "WeaviateNetworkIntrospectActions",
                      description: function() {
                        return getDesc("WeaviateNetworkIntrospectActions")},
                      args: NetworkIntrospectWhereFilterFields,
                      type: new GraphQLList(new GraphQLObjectType({
                        name: "WeaviateNetworkIntrospectActionsObj",
                        description: function() {
                          return getDesc("WeaviateNetworkIntrospectActionsObj")},
                        fields: NetworkIntrospectThingsActionsFields
                      })),
                      resolve(parentValue, args) {
                        console.log("resolve WeaviateNetworkIntrospectActions")
                        return demoResolver.resolveNetworkIntrospect(args) // resolve with empty array
                      }
                    },
                    Beacon: {
                      name: "WeaviateNetworkIntrospectBeacon",
                      description: function() {
                        return getDesc("WeaviateNetworkIntrospectBeacon")},
                      args: {
                        id: { // The id of the beacon like: weaviate://foo-bar-baz/UUID
                          name: "WeaviateNetworkIntrospectBeaconId",
                          description: function() {
                            return getDesc("WeaviateNetworkIntrospectBeaconId")},
                          type: new GraphQLNonNull(GraphQLString)
                        }
                      },
                      type: new GraphQLObjectType({
                        name: "WeaviateNetworkIntrospectBeaconObj",
                        description: function() {
                          return getDesc("WeaviateNetworkIntrospectBeaconObj")},
                        fields: NetworkIntrospectBeaconFields
                      }),
                      resolve(parentValue, args) {
                        console.log("resolve WeaviateNetworkIntrospectBeacon")
                        return demoResolver.resolveNetworkIntrospectBeacon(args) // resolve with empty array
                      }
                    }
                  }
                }),
                resolve() {
                  console.log("resolve WeaviateNetworkIntrospect")
                  return [{}] // resolve with empty array
                }
              },
              GetMeta: {
                name: "WeaviateNetworkGetMeta",
                description: function() {
                  return getDesc("WeaviateNetworkGetMeta")},
                args: {
                  where: { 
                    name: "WeaviateNetworkGetMetaWhere",
                    description: function() {
                      return getDesc("WeaviateNetworkGetMetaWhere")},
                    type: new GraphQLInputObjectType({
                      name: "WeaviateNetworkGetMetaWhereInpObj",
                      description: function() {
                        return getDesc("WeaviateNetworkGetMetaWhereInpObj")},
                      fields: whereFields
                    }) 
                }
                },
                type: new GraphQLObjectType({
                  name: "WeaviateNetworkGetMetaObj",
                  description: function() {
                    return getDesc("WeaviateNetworkGetMetaObj")},
                  fields: WeaviateNetworkGetMetaFields
                }),
                resolve(parentValue, args) {
                  console.log("resolve WeaviateNetworkGetMeta")
                  result = demoResolver.resolveNetworkGet(args.where)
                  if (result != 'error') {
                    return result
                  }
                  else {throw new Error('Text values cannot be filtered because they are not indexed.')}
                },
              }
            }
          })
        }
      }
    })

    // publish the schemas, for now only the query schema
    const schema = new GraphQLSchema({
      name: "RootQuery",
      query: Weaviate
    });

    // run the webserver
    const app = express();
    app.use(cors());
    app.use(express.static(__dirname));
    const graphQLHandler = graphqlHTTP(() => ({ schema, graphiql: true }))
    app.use('/graphql', graphQLHandler);
    app.use('/weaviate/v1/graphql', graphQLHandler)
    app.listen(8081, function() {
      const port = this.address().port;
      console.log(`Started on http://localhost:${port}/graphql`);
    });

  });

});
