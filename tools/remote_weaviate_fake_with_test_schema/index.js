const express = require('express')
const things = require('./test-thing-schema.json')
const actions = require('./test-action-schema.json')

const app = express();
app.get('/weaviate/v1/schema', (req, res) => {
  res.send({
    actions,
    things,
  })
})

app.listen(8082, function() {
  const port = this.address().port;
  console.log(`Started on http://localhost:${port}/graphql`);
});
