# crypt

## Install

### Binary release

```
wget https://github.com/xordataexchange/crypt/releases/download/v0.0.1/crypt-0.0.1-linux-amd64
mv crypt-0.0.1-linux-amd64 /usr/local/bin/crypt
chmod +x /usr/local/bin/crypt
```

### go install

```
go install github.com/xordataexchange/crypt/bin/crypt
```

## Backends

crypt supports etcd and consul as backends via the `-backend` flag.

## Usage

```
usage: crypt COMMAND [arg...]

commands:
   get  retrieve the value of a key
   set  set the value of a key
```

### Encrypted and set a value

```
usage: crypt set [args...] key file
  -backend="etcd": backend provider
  -endpoint="": backend url
  -keyring=".pubring.gpg": path to armored public keyring
```

Example:

```
crypt set -keyring pubring.gpg /app/config config.json 
```

### Retrieve and decrypted a value

```
usage: crypt get [args...] key
  -backend="etcd": backend provider
  -endpoint="": backend url
  -secret-keyring=".secring.gpg": path to armored secret keyring
```

Example:

```
crypt get -secret-keyring secring.gpg /app/config
```

### Support for unencrypted values
```
crypt set -plaintext ...
crypt get -plaintext ...
```
Crypt now has support for getting and setting plain unencrypted values, as
a convenience.  This was added to the backend libraries so it could be exposed
in spf13/viper. Use the -plaintext flag to get or set a value without encryption. 
