> We are updating our docs and they will be moved to [www.semi.technology](https://www.semi.technology) soon.
> You can leave your email [here](http://eepurl.com/gye_bX) to get a notification when they are live.

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

### Read-Only list (introduced in 0.15.0)

Version 0.15.0 adds a new functionality to the Admin List plugin: Other than a
list of admins, it is now also possible to specify a list of read-only users.
Those users have permissions on all `get` and `list` operations, but no other
permissions.

If a subject is present on both the admin and read-only list, weaviate will
error on startup due to the invalid configuration.

### Usage

Simply configure the admin plugin in the config yaml like so:

```yaml
authorization:
  admin_list:
    enabled: true
    users:
      - jane@doe.com
      - john@doe.com
    read_only_users:      # only available from 0.15.0 on, ignored in prior versions
      - roberta@doe.com   # only available from 0.15.0 on, ignored in prior versions
```

The above would enable the plugin and make users `jane@doe.com` and
`john@doe.com` admins. If at least weaviate version `0.15.0` is used,
additionally user `roberta@doe.com` will have read-only permissions.

Note that in the above example email ids are used to
identify the user. This is not a requirement, in fact, any string can be used.
This depends on what you configured in the authentication settings. For
example, if you are using Open ID Connect authentication, you could set the
`authentication.oidc.username_claim` to `email` to achieve the result shown
above.

## RBAC
More fine-grained Role-Based Access Control (RBAC) coming soon. As of know the
only possible distinction is between Admins (CRUD), Read-Only Users and
entirely unauthorized users.
