# Remote Weaviate Fake to use in acceptance tests

For the local dev setup we can use the NodeJS prototype as a fake for the remote
instance. However, in acceptance tests we need more stability. That's the point
of this fake.

It doesn't really understand GraphQL, it just pretends to. It quecks for a few exact queries
and either responds successfully or not based on them.
