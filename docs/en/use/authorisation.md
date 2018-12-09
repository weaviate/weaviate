# Authorisation

> SHORT DESCRIPTION OF THIS DOC

The following scheme is a simple authorisation tree scheme used for authorizing things and actions.

#### Why this scheme?

The goal is to make communication as simple as possible. You can have multiple use-cases based on this scheme. For example: a branch of keys can represent location, time, individuals, etcetera.

#### Definitions & Design

1. There is one root key (accompanied by a token).
2. With every key, a child key can be generated.
3. A child key can have write, read, delete, and execution rights.
4. A parent has access to a child, a child its children, etcetera.
5. A child has no access to a parent.
6. A child can have an expiration timestamp which, when expired, all children expire too.
7. A child can inherit all values from a parent except for the actual key.

#### Miscellaneous

- An object can have multiple keys.
- If you want to have a parent that can't write, read, delete or execute on a child. You can set these values to `false` and within the child to `true`.
- This is [_not_](https://serverfault.com/a/57082) and authentication but an authorisation scheme.