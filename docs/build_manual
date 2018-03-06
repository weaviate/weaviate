# Documentation

+ [Quickstart](#Quickstart);
+ [Build container](#Build);
    + [Build argumetns](#Table1);
    + [Examples](#Ex1);
+ [Run single container](#Run);
    + [Environment variables](#Table2);
    + [Examples](#Ex2);
+ [Docker compose](#DC);
    + [Examples](#Ex3);

## <a name="Quickstart"></a> Quickstart

Clone this repository in your directory.

```sh
$ cd workdir
$ git clone https://github.com/unicanova/weaviate

```

If necessary, edit `docker-compose.yml` file for your situation and write:

```sh
$ docker-compose up

```

Images will be created and containers automatically launched. The application will be available on the port you specified (or **80** if you did not change)

## <a name="Build"></a> Build container

To build docker image with weaviate app you have to execute in the parent folder **docker build -t reponame:version .**

A docker image can be created in 2 ways:

* a. with the use of default values;

* b. with redefinition of arguments. You can redefine as many arguments as you want.

#### <a name="Table1"></a> Build arguments

| Arg | Description | Default value | 
| --- | ----------- | ------------- |
| config | Path to weaviate.conf.json file | ./weaviate.conf.json | 
| action_scheme | Path to file with action schema | ./schema/test-action-schema.json |
| thing_schema | Path to file with thing schema | ./schema/test-thing-schema.json |
| release | Relase name | nightly |
| platform | Platform name | linux |
| architecture | Operating systems architecture | amd64 |
 
### <a name="Ex1"></a> Examples

```sh
a.$ docker build -t <tag> .

b.$ docker build -t <tag> --build-arg release=nightly --build-arg platform=linux --build-arg=386 .
```

## <a name="Run"></a> Run single container

The container can also be launched in two ways:

* a. with the use of default values of environment variables.

* b. with redefinition of environment variables. As well as arguments, the number of predefined environment variables is unlimited.

#### <a name="Table2"></a> Environment variables

| Variable name | Variable value | Default Value |
| ------------- | -------------- | ------------- |
| WEAVIATE_SCHEME | The listeners to enable, this can be repeated and defaults to the schemes in the swagger spec | http |
| WEAVIATE_PORT | The port to listen on for insecure connections | 80 |
| WEAVIATE_HOST | The IP to listen on | localhost |
| WEAVIATE_CONFIG | The section inside the config file that has to be used | cassandra |
| WEAVIATE_CONFIG_FILE | Path to config file | ./weaviate.conf.json |
| CQLVERSION | Specify a particular CQL version | 3.4.4 |

### <a name="Ex2"></a> Examples 

```sh
a. $ docker run <container_name>

b. $ docker run -e WEAVIATE_SCHEME=https -e WEAVIATE_PORT=602 <container_name>"
```

## <a name="DC"></a> Docker-compose


To start two containers at once, you need to use the **docker-compose up** command. 
You can override the environment variables (environment section) and arguments (args section) in the corresponding section of the *docker-compose.yml* file before executing the command.

### <a name="Ex3"></a> Examples 

```sh
$ docker-compose up
```
