var chakram = require('chakram');
var expect = chakram.expect;

describe("Weaviate", function () {
  it("does not serve anything on it's root URL path (i.e. 404)", function () {
    var response = chakram.get("http://service");
    expect(response).to.have.status(404);
    expect(response).to.have.header("content-type", "text/html; charset=utf-8");
    // expect(response).to.comprise.of.json({
    //   args: { test: "chakram" }
    // });
    return chakram.wait();
  });
});
