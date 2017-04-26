'use strict';
/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 Weaviate. All rights reserved.
 * LICENSE: https:///blob/master/LICENSE
 * AUTHOR: Bob van Luijt (bob@weaviate.com)
 * See www.weaviate.com for details
 * See package.json for author and maintainer info
 * Contact: @weaviate_iot / yourfriends@weaviate.com
 */

var   assert  = require('assert'),
      request = require('supertest'),
      should  = require('should');

var   weaviateUrl   = 'http://127.0.0.1:8080/weaviate/v1-alpha/';

/**
 * General tests
 */
var counter = 0;

describe('Testing all weaviate commands', function(){

    /********************************************************************************************
     * Test weaviate.adapters.list
     **/

    /**
     * Test JSON
     */
    it((counter++) + '/84 weaviate.adapters.list (JSON)', function(done){
        request(weaviateUrl)
            .get('/adapters')
            .set('Accept', 'application/json')
            .expect('Content-Type', /json/)
            .expect(501)
            .end(function (err, res) {
                res.status.should.be.equal(501);
                done();
            });
    });

    /**
     * Test XML
     */
    it((counter++) + '/84 weaviate.adapters.list (XML)', function(done){
        request(weaviateUrl)
            .get('/adapters')
            .set('Accept', 'application/xml')
            .expect('Content-Type', /xml/)
            .expect(501)
            .end(function (err, res) {
                res.status.should.be.equal(501);
                done();
            });
    });

    /********************************************************************************************
     * Test weaviate.adapters.insert
     **/

    /**
     * Test JSON
     */
    it((counter++) + '/84 weaviate.adapters.insert (JSON)', function(done){
        request(weaviateUrl)
            .post('/adapters')
            .set('Accept', 'application/json')
            .send({}) // empty for now
            .expect('Content-Type', /json/)
            .expect(501)
            .end(function (err, res) {
                res.status.should.be.equal(501);
                done();
            });
    });

    /**
     * Test XML
     */
    it((counter++) + '/84 weaviate.adapters.insert (XML)', function(done){
        request(weaviateUrl)
            .post('/adapters')
            .set('Accept', 'application/xml')
            .send({}) // empty for now
            .expect('Content-Type', /xml/)
            .expect(501)
            .end(function (err, res) {
                res.status.should.be.equal(501);
                done();
            });
    });

    /********************************************************************************************
     * Test weaviate.adapters.delete
     **/

    /**
     * Test JSON
     */
    it((counter++) + '/84 weaviate.adapters.delete (JSON)', function(done){
        request(weaviateUrl)
            .delete('/adapters/1')
            .set('Accept', 'application/json')
            .expect('Content-Type', /json/)
            .expect(501)
            .end(function (err, res) {
                res.status.should.be.equal(501);
                done();
            });
    });

    /**
     * Test XML
     */
    it((counter++) + '/84 weaviate.adapters.delete (XML)', function(done){
        request(weaviateUrl)
            .delete('/adapters/1')
            .set('Accept', 'application/xml')
            .expect('Content-Type', /xml/)
            .expect(501)
            .end(function (err, res) {
                res.status.should.be.equal(501);
                done();
            });
    });

    /********************************************************************************************
     * Test weaviate.adapters.get
     **/

    /**
     * Test JSON
     */
    it((counter++) + '/84 weaviate.adapters.get (JSON)', function(done){
        request(weaviateUrl)
            .get('/adapters/1')
            .set('Accept', 'application/json')
            .expect('Content-Type', /json/)
            .expect(501)
            .end(function (err, res) {
                res.status.should.be.equal(501);
                done();
            });
    });

    /**
     * Test XML
     */
    it((counter++) + '/84 weaviate.adapters.get (XML)', function(done){
        request(weaviateUrl)
            .get('/adapters/1')
            .set('Accept', 'application/xml')
            .expect('Content-Type', /xml/)
            .expect(501)
            .end(function (err, res) {
                res.status.should.be.equal(501);
                done();
            });
    });

    /********************************************************************************************
     * Test weaviate.adapters.insert
     **/

    /**
     * Test JSON
     */
    it((counter++) + '/84 weaviate.adapters.update (JSON)', function(done){
        request(weaviateUrl)
            .put('/adapters/1')
            .set('Accept', 'application/json')
            .send({}) // empty for now
            .expect('Content-Type', /json/)
            .expect(501)
            .end(function (err, res) {
                res.status.should.be.equal(501);
                done();
            });
    });

    /**
     * Test XML
     */
    it((counter++) + '/84 weaviate.adapters.update (XML)', function(done){
        request(weaviateUrl)
            .put('/adapters/1')
            .set('Accept', 'application/xml')
            .send({}) // empty for now
            .expect('Content-Type', /xml/)
            .expect(501)
            .end(function (err, res) {
                res.status.should.be.equal(501);
                done();
            });
    });

    /********************************************************************************************
     * Test weaviate.adapters.patch
     **/

    /**
     * Test JSON
     */
    it((counter++) + '/84 weaviate.adapters.patch (JSON)', function(done){
        request(weaviateUrl)
            .patch('/adapters/1')
            .set('Accept', 'application/json')
            .send({}) // empty for now
            .expect('Content-Type', /json/)
            .expect(501)
            .end(function (err, res) {
                res.status.should.be.equal(501);
                done();
            });
    });

    /**
     * Test XML
     */
    it((counter++) + '/84 weaviate.adapters.patch (XML)', function(done){
        request(weaviateUrl)
            .patch('/adapters/1')
            .set('Accept', 'application/xml')
            .send({}) // empty for now
            .expect('Content-Type', /xml/)
            .expect(501)
            .end(function (err, res) {
                res.status.should.be.equal(501);
                done();
            });
    });

});
