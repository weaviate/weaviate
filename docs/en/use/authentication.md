# Authentication

> About authentication in Weaviate

## Philosophy

Weaviate should be as easy-to-use as possible regardless of the setting. Trying
it out locally can be a very different case than running it in production in an
enterprise environment.

We want to make sure that Authentication reflects this. Thus, different
authentication schemes can be selected and even combined. This allows scenarios
such as "Anonymous users, can read some resources, but not all. Authenticated
users can read all resources. Only a special group can write or delete
resources."

## Available Authentication schemes

Currently Anonymous Access and OpenID Connect are supported. Other
Authentication schemes might become available in the near future.

If all authentication schemes - including anonymous access - are disabled,
weaviate will fail to start up and ask you to configure at least one.

### Anonymous Access
If anonymous access is selected, weaviate will accept requests without any
authentication headers or parameters. Users sending such a request will be
authenticated as `user: anyonmous, group: anonymous`.

It is up to the [Authorization module](./authorization.md) to decide which
permissions anonymous users have. By disabling anonymous access alltogether,
any request without an allowed authentication scheme, will return 401.

#### Configuration
Anonymous Access can be configured like so in the respective environment in the
`configuration.yaml`:

*Note that this example uses yaml, however configuration can also be written as
JSON*

```yaml
  authentication:
    anonymous_access:
      enabled: true
```

#### How to use

Simply omit any authentication headers or parameters from your REST request to
weaviate.

### OpenID Connect (OIDC)

With [OpenID Connect](https://openid.net/connect/) (based on OAuth2) an
external identity provider and token issuer is responsible for managing users.
Weaviate's part when receiving a token (JSON Web Token or JWT) is to verify
that it was indeed signed by the configured token issuer. If the signature is
correct all content of the token is trusted. The user is then authenticated as
the subject mentioned in the token.

#### Requirements &amp; Defaults

- Any "OpenID Connect" compatible token issuer implementing [OpenID Connect
  Discovery](https://openid.net/specs/openid-connect-discovery-1_0.html) can be
  used with weaviate. Popular open source solutions include Java-based
  [Keycloak](https://www.keycloak.org/) and Golang-based
  [dex](https://github.com/dexidp/dex).

- By default, weaviate will validate that the token includes a specified client
  id in the audience claim. If your token issuer does not support this feature,
  you can turn it off as outlined in the configuration section below.

- By default, weaviate will try to extract groups from a claim of your choice.
  Groups are a helpful tool to implement authorization roles for groups rather
  than single people. However, groups are not a requied OpenID spec. Therefore
  this extraction is option and will not fail authentication if no groups could
  be found.

#### Configuration

OpenID Connect (OIDC) can be configured like so in the respective environment in the
`configuration.yaml`. Please see the inline yaml-comments for details around the respective fields:

*Note that this example uses yaml, however configuration can also be written as
JSON*

```yaml
    oidc:
      # enabled turns on OIDC auth 
      enabled: true

      # issuer (required) tells weaviate how to discover the token issuer. This
      # endpoint must implement the OpenID Connect Disovery spec, so that weaviate
      # can retrieve the Issuer's public key.
      #
      # The example URL below uses the path structure commonly found with keycloak
      # where an example realm 'my-weaviate-usecase' was created. The exact
      # path structure, will depend on the token issuer of your choice. Please
      # see the respective documentation about which endpoint implements OIDC
      # Discovery.
      issuer: http://my-token-issuer/auth/realms/my-weaviate-usecase

      # username_claim (required) tells weaviate which claim to use for extracting
      # the username. The username will be passed to the authorization module.
      username_claim: email

      # groups_claim (optional) tells weaviate which claim to use for extracting
      # the groups. Groups must be an array of string. If groups_claim is not set
      # weaviate will not try to extract groups and pass an empty array to the 
      # authorization module.
      groups_claim: groups

      # client_id (required unless skip_client_id_check is set to true) tells 
      # weaviate to check for a particular OAuth 2.0 client id in the audience claim.
      # This is to prevent that a token which was signed by the correct issuer
      # but never intended to be used with weaviate can be used for authentication.
      #
      # For more information on what clients are in OAuth 2.0, see
      # https://tools.ietf.org/html/rfc6749#section-1.1
      client_id: my-weaviate-client

      # skip_client_id_check (optional, defaults to false) skips the client_id
      # validation in the audience claim as outlined in the section above.
      # Not recommended to set this option as it reduces security, only set this
      # if your token issuer is enable to provide a correct audience claim
      skip_client_id_check: false
```

