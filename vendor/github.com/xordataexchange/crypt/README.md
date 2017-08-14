# crypt

You can use crypt as a command line tool or as a configuration library:

* [crypt cli](bin/crypt)
* [crypt/config](config)

## Demo

Watch Kelsey explain `crypt` in this quick 5 minute video:

[![Crypt Demonstration Video](https://img.youtube.com/vi/zYpqqfuGwW8/0.jpg)](https://www.youtube.com/watch?v=zYpqqfuGwW8)

## Generating gpg keys and keyrings

The crypt cli and config package require gpg keyrings. 

### Create a key and keyring from a batch file

```
vim app.batch
```

```
%echo Generating a configuration OpenPGP key
Key-Type: default
Subkey-Type: default
Name-Real: app
Name-Comment: app configuration key
Name-Email: app@example.com
Expire-Date: 0
%pubring .pubring.gpg
%secring .secring.gpg
%commit
%echo done
```

Run the following command:

```
gpg2 --batch --armor --gen-key app.batch
```

You should now have two keyrings, `.pubring.gpg` which contains the public keys, and `.secring.gpg` which contains the private keys.

> Note the private key is not protected by a passphrase.
