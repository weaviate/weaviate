import uuid

import pytest
import weaviate
import weaviate.classes as wvc
from _pytest.fixtures import SubRequest
from weaviate.collections.classes.data import DataReference
from weaviate.rbac.models import RBAC

from .conftest import _sanitize_role_name, role_wrapper, Role_Wrapper_Type

pytestmark = pytest.mark.xdist_group(name="rbac")


def test_rbac_viewer_assign(
    request: SubRequest, admin_client, viewer_client, role_wrapper: Role_Wrapper_Type
):
    name = _sanitize_role_name(request.node.name)

    admin_client.collections.delete(name)
    admin_client.collections.create(name=name)

    # cannot delete with viewer permissions
    with pytest.raises(weaviate.exceptions.UnexpectedStatusCodeException) as e:
        viewer_client.collections.delete(name)

    # with extra role that has those permissions it works
    with role_wrapper(admin_client, request, RBAC.permissions.config.delete(collection=name)):
        viewer_client.collections.delete(name)

    admin_client.collections.delete(name)
