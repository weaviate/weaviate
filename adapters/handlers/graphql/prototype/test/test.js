var assert = require('assert');
var request = require('request');
var lodash = require('lodash');
var should = require('should');
var expect = require('expect');

function doGraphQLRequest(requestBody, callback){

    request.post({
            uri: 'http://localhost:8081/graphql',
            method: 'POST',
            json: {
                "query": requestBody
            }
        },
        function (error, response, body) {            
            callback(error, response, body)
        }
    );

}

//
// START THE TESTS
//
describe('Integration Tests', function () {

    describe('No query', function() {
        // TEST 1
        it('should return an error with a string message: "must provide query string.", with statuscode 400', function(done) {
            doGraphQLRequest(``, function(error, response, resultBody){
                // validate the test
                assert.equal(resultBody.errors[0].message, "Must provide query string.");
                assert.equal(response.statusCode, 400)
                done();
            })
        });
    });
});



describe('Unit Tests', function() {
    describe('Local', function() {

        describe('ConvertedFetch Nested', function() {
            // TEST 1
            it('should return data.Local.ConvertedFetch.Things.City with an array', function(done) {
                doGraphQLRequest(`
                {
                    Local {
                        ConvertedFetch{
                            Things{
                                City{
                                    name
                                    population
                                    isCapital
                                }
                            }
                        }
                    }
                }
                `, function(error, response, resultBody){
                    // validate the test
                    assert.equal(Array.isArray(resultBody.data.Local.ConvertedFetch.Things.City), true);
                    done();
                })
            });

            // TEST 1
            it('should return data.Local.ConvertedFetch.Things.City[0] with an object', function(done) {
                doGraphQLRequest(`
                {
                    Local {
                        ConvertedFetch{
                            Things{
                                City{
                                    name
                                    population
                                    isCapital
                                }
                            }
                        }
                    }
                }
                `, function(error, response, resultBody){
                    // validate the test
                    assert.equal(typeof resultBody.data.Local.ConvertedFetch.Things.City, "object");
                    done();
                })
            });

            // TEST 2
            it('should return data.Local.ConvertedFetch.Things.City[0] with name = string', function(done) {
                doGraphQLRequest(`
                {
                    Local {
                        ConvertedFetch {
                            Things {
                                City {
                                    name
                                    population
                                    isCapital
                                }
                            }
                        }
                    }
                }              
                `, function(error, response, resultBody){
                    // validate the test
                    assert.equal(typeof resultBody.data.Local.ConvertedFetch.Things.City[0].name, "string");
                    done();
                })
            });

            // TEST 3
            it('should return data.Local.ConvertedFetch.Things.City[0] with population = number', function(done) {
                doGraphQLRequest(`
                {
                    Local {
                        ConvertedFetch {
                            Things {
                                City {
                                    name
                                    population
                                    isCapital
                                }
                            }
                        }
                    }
                }              
                `, function(error, response, resultBody){
                    // validate the test
                    assert.equal(typeof resultBody.data.Local.ConvertedFetch.Things.City[0].population, "number");
                    done();
                })
            });
            
            // TEST 4
            it('should return data.Local.ConvertedFetch.Things.City[0] with isCapital = boolean', function(done) {
                doGraphQLRequest(`
                {
                    Local {
                        ConvertedFetch {
                            Things {
                                City {
                                    name
                                    population
                                    isCapital
                                }
                            }
                        }
                    }
                }              
                `, function(error, response, resultBody){
                    // validate the test
                    assert.equal(typeof resultBody.data.Local.ConvertedFetch.Things.City[0].isCapital, "boolean");
                    done();
                })
            });
            
            // TEST 5
            it('should return data.Local.ConvertedFetch.Things.Person[0] with LivesIn = string', function(done) {
                doGraphQLRequest(`
                {
                    Local {
                        ConvertedFetch {
                            Things {
                                Person {
                                    LivesIn {
                                        ...on City {
                                            name
                                        }
                                    }
                                    birthday
                                }
                            }
                        }
                    }
                }              
                `, function(error, response, resultBody){
                    // validate the test
                    assert.equal(typeof resultBody.data.Local.ConvertedFetch.Things.Person[0].LivesIn.name, "string");
                    done();
                })
            });


        });
        
        describe('ConvertedFetch Filters', function() {

            // TEST 
            it('should return for each data.Local.ConvertedFetch.Things.Person with LivesIn.name === "Amsterdam"', function(done) {
                doGraphQLRequest(`
                {
                    Local{
                      ConvertedFetch(_filter: {
                        EQ: [{
                          path: ["Things", "Person", "livesIn", "City", "name"],
                          value: "Amsterdam"
                        }]
                      }){
                        Things{
                          Person{
                            LivesIn{
                              ...on City{
                                name
                              }
                            }
                          }
                        }
                      }
                    }
                  }          
                `, function(error, response, resultBody){
                    // validate the test
                    for (var i in resultBody.data.Local.ConvertedFetch.Things.Person) {
                        assert.equal(resultBody.data.Local.ConvertedFetch.Things.Person[i].LivesIn.name, "Amsterdam");
                    }
                    done();
                })
            });

            // TEST 
            it('should return for each data.Local.ConvertedFetch.Things.Person with LivesIn.name !== "Amsterdam"', function(done) {
                doGraphQLRequest(`
                {
                    Local{
                      ConvertedFetch(_filter: {
                        NEQ: [{
                          path: ["Things", "Person", "livesIn", "City", "name"],
                          value: "Amsterdam"
                        }]
                      }){
                        Things{
                          Person{
                            LivesIn{
                              ...on City{
                                name
                              }
                            }
                          }
                        }
                      }
                    }
                  }          
                `, function(error, response, resultBody){
                    // validate the test
                    for (var i in resultBody.data.Local.ConvertedFetch.Things.Person) {
                        assert.notEqual(resultBody.data.Local.ConvertedFetch.Things.Person[i].LivesIn.name, "Amsterdam");
                    }
                    done();
                })
            });

            // TEST 
            it('should return for each data.Local.ConvertedFetch.Things.Person with LivesIn.population > 1500000"', function(done) {
                doGraphQLRequest(`
                {
                    Local{
                      ConvertedFetch(_filter: {
                        IE: [{
                          path: ["Things", "Person", "livesIn", "City", "population"],
                          value: ">1500000"
                        }]
                      }){
                        Things{
                          Person{
                            LivesIn{
                              ...on City{
                                population
                              }
                            }
                          }
                        }
                      }
                    }
                  }          
                `, function(error, response, resultBody){
                    // validate the test
                    for (var i in resultBody.data.Local.ConvertedFetch.Things.Person) {
                        assert(resultBody.data.Local.ConvertedFetch.Things.Person[i].LivesIn.population > 1500000);
                    }
                    done();
                })
            });

            // TEST 
            it('should return for each data.Local.ConvertedFetch.Things.Person with LivesIn.population < 1500000"', function(done) {
                doGraphQLRequest(`
                {
                    Local{
                      ConvertedFetch(_filter: {
                        IE: [{
                          path: ["Things", "Person", "livesIn", "City", "population"],
                          value: "<1500000"
                        }]
                      }){
                        Things{
                          Person{
                            LivesIn{
                              ...on City{
                                population
                              }
                            }
                          }
                        }
                      }
                    }
                  }          
                `, function(error, response, resultBody){
                    // validate the test
                    for (var i in resultBody.data.Local.ConvertedFetch.Things.Person) {
                        assert(resultBody.data.Local.ConvertedFetch.Things.Person[i].LivesIn.population < 1500000);
                    }
                    done();
                })
            });

            // TEST 
            it('should return for each data.Local.ConvertedFetch.Things.Person with LivesIn.population => 1300000"', function(done) {
                doGraphQLRequest(`
                {
                    Local{
                      ConvertedFetch(_filter: {
                        IE: [{
                          path: ["Things", "Person", "livesIn", "City", "population"],
                          value: "=>1300000"
                        }]
                      }){
                        Things{
                          Person{
                            LivesIn{
                              ...on City{
                                population
                              }
                            }
                          }
                        }
                      }
                    }
                  }          
                `, function(error, response, resultBody){
                    // validate the test
                    for (var i in resultBody.data.Local.ConvertedFetch.Things.Person) {
                        assert(resultBody.data.Local.ConvertedFetch.Things.Person[i].LivesIn.population >= 1300000);
                    }
                    done();
                })
            });

            // TEST 
            it('should return for each data.Local.ConvertedFetch.Things.Person with LivesIn.population <= 1300000"', function(done) {
                doGraphQLRequest(`
                {
                    Local{
                      ConvertedFetch(_filter: {
                        IE: [{
                          path: ["Things", "Person", "livesIn", "City", "population"],
                          value: "=<1300000"
                        }]
                      }){
                        Things{
                          Person{
                            LivesIn{
                              ...on City{
                                population
                              }
                            }
                          }
                        }
                      }
                    }
                  }          
                `, function(error, response, resultBody){
                    // validate the test
                    for (var i in resultBody.data.Local.ConvertedFetch.Things.Person) {
                        assert(resultBody.data.Local.ConvertedFetch.Things.Person[i].LivesIn.population <= 1300000);
                    }
                    done();
                })
            });

            // TEST 
            it('should return for each data.Local.ConvertedFetch.Things.Airport with inCity.name == "Rotterdam" AND inCity.inCountry.Country.name == "Netherlands"', function(done) {
                doGraphQLRequest(`
                {
                  Local{
                    ConvertedFetch(_filter:{
                      EQ: [{
                        path: ["Things", "Airport", "inCity", "City", "inCountry", "Country", "name"],
                        value: "Netherlands"
                      }, {
                        path: ["Things", "Airport", "inCity", "City", "name"],
                        value: "Rotterdam"
                      }],
                    }){
                      Things{
                        Airport{
                          code
                          name
                          InCity{
                            ...on City{
                              name
                              population
                              coordinates
                              isCapital
                              InCountry{
                                ...on Country{
                                  name
                                  population
                                }
                              }
                            }
                          }
                        }
                      }
                    }
                  }
                }
                `, function(error, response, resultBody){
                    // validate the test
                    for (var i in resultBody.data.Local.ConvertedFetch.Things.Airport) {
                        assert((resultBody.data.Local.ConvertedFetch.Things.Airport[i].InCity.name, "Rotterdam") && (resultBody.data.Local.ConvertedFetch.Things.Airport[i].InCity.InCountry.name, "Netherlands"));
                    }
                    done();
                })
            });

            // TEST 
            it('should return for each data.Local.ConvertedFetch.Things.Airport with inCity.name !== "Rotterdam" AND inCity.inCountry.Country.name !== "Netherlands"', function(done) {
                doGraphQLRequest(`
                {
                  Local{
                    ConvertedFetch(_filter:{
                      NEQ: [{
                        path: ["Things", "Airport", "inCity", "City", "inCountry", "Country", "name"],
                        value: "Netherlands"
                      }, {
                        path: ["Things", "Airport", "inCity", "City", "name"],
                        value: "Rotterdam"
                      }],
                    }){
                      Things{
                        Airport{
                          code
                          name
                          InCity{
                            ...on City{
                              name
                              population
                              coordinates
                              isCapital
                              InCountry{
                                ...on Country{
                                  name
                                  population
                                }
                              }
                            }
                          }
                        }
                      }
                    }
                  }
                }
                `, function(error, response, resultBody){
                    // validate the test
                    for (var i in resultBody.data.Local.ConvertedFetch.Things.Airport) {
                        assert((resultBody.data.Local.ConvertedFetch.Things.Airport[i].InCity.name !== "Rotterdam") && (resultBody.data.Local.ConvertedFetch.Things.Airport[i].InCity.InCountry.name !== "Netherlands"));
                    }
                    done();
                })
            });

            // TEST 
            it('should return for each data.Local.ConvertedFetch.Actions.MoveAction with ToCity.population > 1500000 AND ToCity.population < 1500000', function(done) {
                doGraphQLRequest(`
                {
                    Local{
                      ConvertedFetch(_filter: {
                        IE: [{
                          path: ["Actions", "MoveAction", "toCity", "population"],
                          value: ">1500000"
                        }, {
                          path: ["Actions", "MoveAction", "fromCity", "population"],
                          value: "<1500000"
                        }]
                      }){
                        Actions{
                          MoveAction{
                            ToCity{
                              ...on City{
                                population
                              }
                            }
                            FromCity{
                              ...on City{
                                population
                              }
                            }
                          }
                        }
                      }
                    }
                  }
                `, function(error, response, resultBody){
                    // validate the test
                    for (var i in resultBody.data.Local.ConvertedFetch.Actions.MoveAction) {
                        assert((resultBody.data.Local.ConvertedFetch.Actions.MoveAction[i].ToCity.population > 1500000) && (resultBody.data.Local.ConvertedFetch.Actions.MoveAction[i].FromCity.population < 1500000));
                    }
                    done();
                })
            });

            // TEST 
            it('should return for each data.Local.ConvertedFetch.Actions.MoveAction with ToCity.name == "Amsterdam" OR FromCity.name == "Rotterdam"', function(done) {
                doGraphQLRequest(`
                {
                    Local{
                      ConvertedFetch(_filter: {
                        EQ: [{
                          path: ["Actions", "MoveAction", "toCity", "name"],
                          value: "Amsterdam"
                        }, {
                          path: ["Actions", "MoveAction", "fromCity", "name"],
                          value: "Rotterdam"
                        }]
                      }){
                        Actions{
                          MoveAction{
                            ToCity{
                              ...on City{
                                name
                              }
                            }
                            FromCity{
                              ...on City{
                                name
                              }
                            }
                          }
                        }
                      }
                    }
                  }
                `, function(error, response, resultBody){
                    // validate the test
                    for (var i in resultBody.data.Local.ConvertedFetch.Actions.MoveAction) {
                        assert((resultBody.data.Local.ConvertedFetch.Actions.MoveAction[i].ToCity.name == "Amsterdam") || (resultBody.data.Local.ConvertedFetch.Actions.MoveAction[i].FromCity.name == "Rotterdam"))
                    }
                    done();
                })
            });

            // TEST 
            it('should return for each data.Local.ConvertedFetch.Actions.MoveAction with ToCity.name !== "Amsterdam" OR FromCity.name !== "Rotterdam"', function(done) {
                doGraphQLRequest(`
                {
                    Local{
                      ConvertedFetch(_filter: {
                        NEQ: [{
                          path: ["Actions", "MoveAction", "toCity", "name"],
                          value: "Amsterdam"
                        }, {
                          path: ["Actions", "MoveAction", "fromCity", "name"],
                          value: "Rotterdam"
                        }]
                      }){
                        Actions{
                          MoveAction{
                            ToCity{
                              ...on City{
                                name
                              }
                            }
                            FromCity{
                              ...on City{
                                name
                              }
                            }
                          }
                        }
                      }
                    }
                  }
                `, function(error, response, resultBody){
                    // validate the test
                    for (var i in resultBody.data.Local.ConvertedFetch.Actions.MoveAction) {
                        assert((resultBody.data.Local.ConvertedFetch.Actions.MoveAction[i].ToCity.name !== "Amsterdam") || (resultBody.data.Local.ConvertedFetch.Actions.MoveAction[i].FromCity.name !== "Rotterdam"));
                    }
                    done();
                })
            });

            // TEST 
            it('should return for each data.Local.ConvertedFetch.Actions.MoveAction with ToCity.population > 1500000 OR ToCity.population < 1500000', function(done) {
                doGraphQLRequest(`
                {
                    Local{
                      ConvertedFetch(_filter: {
                        IE: [{
                          path: ["Actions", "MoveAction", "toCity", "population"],
                          value: ">1500000"
                        }, {
                          path: ["Actions", "MoveAction", "fromCity", "population"],
                          value: "<1500000"
                        }]
                      }){
                        Actions{
                          MoveAction{
                            ToCity{
                              ...on City{
                                population
                              }
                            }
                            FromCity{
                              ...on City{
                                population
                              }
                            }
                          }
                        }
                      }
                    }
                  }
                `, function(error, response, resultBody){
                    // validate the test
                    for (var i in resultBody.data.Local.ConvertedFetch.Actions.MoveAction) {
                        assert((resultBody.data.Local.ConvertedFetch.Actions.MoveAction[i].ToCity.population > 1500000) || (resultBody.data.Local.ConvertedFetch.Actions.MoveAction[i].FromCity.population < 1500000));
                    }
                    done();
                })
            });

            // TEST 
            it('should return for each data.Local.ConvertedFetch.Things.Person with LivesIn.name == "Amsterdam" OR LivesIn.name !== "Amsterdam" OR LivesIn.population > 1000000', function(done) {
                doGraphQLRequest(`
                {
                    Local{
                      ConvertedFetch(_filter: {
                        EQ: [{
                          path: ["Things", "Person", "livesIn", "City", "name"],
                          value: "Amsterdam"
                        }], 
                        NEQ: [{
                          path: ["Things", "Person", "livesIn", "City", "name"],
                          value: "Rotterdam"
                        }], 
                        IE: [{
                          path: ["Things", "Person", "livesIn", "City", "population"],
                          value: ">1000000"
                        }]
                      }){
                        Things{
                          Person{
                            LivesIn{
                              ...on City{
                                name
                              }
                            }
                          }
                        }
                      }
                    }
                  }
                `, function(error, response, resultBody){
                    // validate the test
                    for (var i in resultBody.data.Local.ConvertedFetch.Things.Person) {
                        assert((resultBody.data.Local.ConvertedFetch.Things.Person[i].LivesIn.name, "Amsterdam") || (resultBody.data.Local.ConvertedFetch.Things.Person[i].LivesIn.name, "Rotterdam") || (resultBody.data.Local.ConvertedFetch.Things.Person[i].LivesIn.population > 1000000));
                    }
                    done();
                })
            });

        });


        describe('WeaviateLocalConvertedFetchObj Filters', function() {

            // TEST 
            it('should return data.Local.ConvertedFetch.Things.City with an array of length 1', function(done) {
                doGraphQLRequest(`
                {
                    Local{
                      ConvertedFetch{
                        Things{
                          City(_limit:1){
                            name
                            latitude
                            population
                            isCapital
                          }
                        }
                      }
                    }
                  }   
                `, function(error, response, resultBody){
                    // validate the test
                    assert(resultBody.data.Local.ConvertedFetch.Things.City.length, 1)
                    done();
                })
            });

            // TEST 
            it('should return data.Local.ConvertedFetch.Things.City with an array where the first item is skipped', function(done) {
                doGraphQLRequest(`
                {
                    Local{
                      ConvertedFetch{
                        Things{
                          City{
                            name
                            latitude
                            population
                            isCapital
                          }
                        }
                      }
                    }
                  }
                `, function(error, response, resultBody){
                    // validate the test
                    total_results = resultBody.data.Local.ConvertedFetch.Things.City

                    doGraphQLRequest(`
                    {
                        Local{
                        ConvertedFetch{
                            Things{
                            City(_skip:1){
                                name
                                latitude
                                population
                                isCapital
                            }
                            }
                        }
                        }
                    }
                    `, function(error, response, resultBody){
                        // validate the test
                        assert.deepEqual(resultBody.data.Local.ConvertedFetch.Things.City, total_results.splice(1))
                        done();
                        })
                })
            });
        });

        describe('WeaviateLocalMetaFetchGenerics Nested', function() {

            // TEST 
            it('should return data.Local.MetaFetch.Generics.Things.City with an object', function(done) {
                doGraphQLRequest(`
                {
                    Local{
                      MetaFetch{
                        Generics{
                          Things{
                            City{
                              meta{
                                counter
                                pointing{
                                  to
                                  from
                                }
                              }
                            }
                          }
                        }
                      }
                    }
                  }
                `, function(error, response, resultBody){
                    // validate the test
                    assert(typeof resultBody.data.Local.MetaFetch.Generics.Things.City, "object")
                    done();
                })
            });

            // TEST 
            it('should return data.Local.MetaFetch.Generics.Things.City with an object with meta.counter, meta.pointing.to and meta.pointing.from as numbers ', function(done) {
                doGraphQLRequest(`
                {
                    Local{
                      MetaFetch{
                        Generics{
                          Things{
                            City{
                              meta{
                                counter
                                pointing{
                                  to
                                  from
                                }
                              }
                            }
                          }
                        }
                      }
                    }
                  }
                `, function(error, response, resultBody){
                    // validate the test
                    assert(typeof resultBody.data.Local.MetaFetch.Generics.Things.City.meta.counter == "number" && typeof resultBody.data.Local.MetaFetch.Generics.Things.City.meta.pointing.to == "number" && typeof resultBody.data.Local.MetaFetch.Generics.Things.City.meta.pointing.from == "number")
                    done();
                })
            });

            // TEST 
            it('should return data.Local.MetaFetch.Generics.Things.City with an object with meta.counter, meta.pointing.to and meta.pointing.from as numbers ', function(done) {
                doGraphQLRequest(`
                {
                    Local{
                      MetaFetch{
                        Generics{
                          Things{
                            City{
                              meta{
                                counter
                                pointing{
                                  to
                                  from
                                }
                              }
                            }
                          }
                        }
                      }
                    }
                  }
                `, function(error, response, resultBody){
                    // validate the test
                    assert(typeof resultBody.data.Local.MetaFetch.Generics.Things.City.meta.counter == "number" && typeof resultBody.data.Local.MetaFetch.Generics.Things.City.meta.pointing.to == "number" && typeof resultBody.data.Local.MetaFetch.Generics.Things.City.meta.pointing.from == "number")
                    done();
                })
            });
        });

        describe('Validate the Object Schemas (graphql introspection)', function() {

            //TEST 4
            it('should contain the correct naming convention: "WeaviateObj"', function(done){
                doGraphQLRequest(`
                {
                    __schema {
                    types {
                        name
                    }
                    }
                }`, function(error, response, resultBody){
                    // lodash is used to find the "name"
                    expect(lodash.filter(resultBody.data.__schema.types, x => x.name === 'WeaviateObj').length).not.toBeLessThan(1);
                    done();
                })
            })

            // TEST 5
            it('should contain the correct naming convention: "WeaviateLocalObj"', function(done){
                doGraphQLRequest(`
                {
                    __schema {
                    types {
                        name
                    }
                    }
                }`, function(error, response, resultBody){
                    // do the test
                    expect(lodash.filter(resultBody.data.__schema.types, x => x.name === 'WeaviateLocalObj').length).not.toBeLessThan(1);
                    done();
                })
            })
        })
    });
});



// describe('ServerSpec', function () {

//     describe('Local', function() {
        
//         describe('ConvertedFetch', function() {

//             describe('Things', function() {

//                 describe('Thing', function() {

//                     describe('Property', function() {
        
        
        
//         // TEST 1
//         it('should return an error with a string message: "must provide query string.", with statuscode 400', function(done) {
//             doGraphQLRequest(``, function(error, response, resultBody){
//                 // validate the test
//                 assert.equal(resultBody.errors[0].message, "Must provide query string.");
//                 assert.equal(response.statusCode, 400)
//                 done();
//             })
//         });
//     });
// });
