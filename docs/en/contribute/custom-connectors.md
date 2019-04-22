# Custom Database Connectors
 
> Documentation on how to build new database connectors.

Weaviate is loosely coupled when it comes to the underlying database. You can
make a tradeoff based on your use case or application. Often you can choose
between speed and scale when running a Weaviate.

If your storage of choice is not available or if you want to create a specific
mixed setup, there are a few simple steps you can take to start creating your
own. If you use the test scripts provided with Weaviate in the
[/test](../../test) directory you can directly validate if your connector works
as expected.

## Getting Started

The most important directory and file you will be working with are;
- The [/database/connectors](../../../database/connectors) directory. And;
- The
  [/usecases/connswitch/connectors.go](../../../usecases/connswitch/connectors.go)
  file.

In the database directory, you will be doing most of your work. The listing
file is only used once to describe the new connector.

## The Foobar Connector

The [foobar](../../../database/connectors/foobar/connector.go) connector is an
empty connector that you can use as a template. We highly recommend copying
this directory when starting on a new connector. By looking at other
connectors, you might see that the methodss are separated into multiple files.
The logic to resolve the data for the individual methods can further be
extracted into subpackages if they deal with isolated concerns. You can do
that with your connector to, but the `foobar/connector.go` file contains all the
functions that you need to populate.

## Making Your Connector Known

If you have created a new connector in the
[/database/connectors/\<sampleconnector\>](../../../database/connectors) folder,
you need to edit the
[/usecases/connswitch/connectors.go](../../../database/connectors/connectors.go)
file to include your connector.

For example, adding a new `sampleconnector` would look like this:

```go
switch name {
    case "janusgraph":
        err, conn = janusgraph.New(config)
    case "sampleconnector":
        err, conn = sampleconnector.New(config)
    case "foobar":
        err, conn = foobar.New(config)
    default:
        err = fmt.Errorf("No connector with the name '%s' exists!", name)
}
```

## Running Weaviate with your connection.

When [running Weaviate](../use/running-weaviate.md), the flag `--config-file=`
needs to be set. In this configuration file you can add the following
parameters:

```go
"database": {
    "name": "sampleconnector",
    "database_config": {
        "host": "localhost",
        "port": 1,
        "whatever_you_want": true
    }
}
```

Misc:<br>
The `database_config` object is marshalled as `json.RawMessage` during the
initial reading of the configuration in weaviate's startup routine. This means
it has no fixed structure outside of your connector. You are free to modify or
add new keys to the object, since you will first unmarshal during your
connector initialization. This configuration becomes available in the `config`
variable inside your connector.

Misc:<br>
- Look at [this](../../../weaviate.conf.json#L4-L10) configuration file to see
  how `"database":{}` is being used.
- See how the config is set in a [docker compose](../../../Dockerfile-prod#L46)
  file.

## Building Your Connector

Every time Weaviate's RESTful API is used or a GraphQL `Local {}` query
resolved, the corresponding method on the connector (all in `connectors.go` in
the `foobar` connector) is called. The [`Foobar` connector](../../../database/connectors/foobar/connector.go)
contains all information to construct a connector and information on when which
methods are called.
