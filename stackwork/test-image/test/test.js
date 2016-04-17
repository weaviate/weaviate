'use strict';
/**                         _       _
 *                         (_)     | |
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 Weaviate. All rights reserved.
 * See www.weaviate.com for details
 * See package.json for author and maintainer info
 * Contact: @weaviate_iot / yourfriends@weaviate.com
 */

const chakram = require('chakram');
const expect = chakram.expect;

describe('Weaviate', function () {
  it('does not serve anything on its root URL path (i.e. 404)', function () {
    var response = chakram.get('http://service');
    expect(response).to.have.status(404);
    expect(response).to.have.header('content-type', 'text/html; charset=utf-8');
    // expect(response).to.comprise.of.json({
    //   args: { test: 'chakram' }
    // });
    return chakram.wait();
  });
});
