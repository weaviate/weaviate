# Authorization

> Authorization in weaviate

Similiar to our philosophy regarding database backends and the overall
authentication scheme, authorization is also implemented in a pluggable
fashion.

This means you can chose the plugin that fits your use case best. If you have
only a few users and don't need to differentiate between their rights, the
"Admin List" plugin is a perfect fit. If you need to control each user's
permissions at a very fine-grained level, however, you should opt to use the
RBAC plugin.

## Admin List

Admin List relies on the configured Authentication Scheme to correctly identify
the user. On each request a check against a pre-configured admin list is done.
If the user is contained on this list, they get all permissions. If they aren't
they get none. It's not possible to assign only some rights to a specific user
with the Admin List plugin.

### Usage

Simply configure the admin plugin in the config yaml like so:

```yaml
authorization:
  admin_list:
    enabled: true
    users:
      - jane@doe.com
      - john@doe.com
```

The above would enable the plugin and make users `jane@doe.com` and
`john@doe.com` admins. Note that in the above example email ids are used to
identify the user. This is not a requirement, in fact, any string can be used.
This depends on what you configured in the authentication settings. For
example, if you are using Open ID Connect authentication, you could set the
`authentication.oidc.username_claim` to `email` to achieve the result shown
above.

## RBAC
Full Role-Based Access Control (RBAC) coming soon. As of now any authenticated
user is fully authorized to read, write, modify or delete any resource.


