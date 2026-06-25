"""weaviate-python-client cross-reference tests against a 3-node, NS cluster.

This file focuses on the client-contract surface — the python-client
serialises refs, target_collection kwargs, ReferenceToMulti, and filter
builders in shapes the raw REST/gRPC paths don't reach. Several bugs we
hit on this branch surfaced specifically because the client encodes
`to=<uuid>` differently from a raw beacon submission.

Storage-shape and isolation invariants (admin REST inspection, admin
delete with qualified beacon, namespaced user cross-NS delete, isolation
between same-UUID rows in two namespaces) are covered in Go under
`test/acceptance/namespace/references_test.go` — they don't need the
python client and are cheaper to run in the standard Go acceptance loop.

CI: wired as the "python-namespaces" matrix entry in pull_requests.yaml
via `./test/run.sh --acceptance-only-python-namespaces`, which builds the
image, ups the 3-node compose, waits for readiness, then runs this file.

Manual run (the python-default suite excludes this file so the standard
compose is enough for the rest):
    docker compose -f docker-compose-namespaces-test.yml up -d
    pytest test/acceptance_with_python/test_namespace_refs.py

# Asymmetry under test

Classes are stored under qualified names (`customer1:Animal`), but
stored cross-ref beacons keep the SHORT class
(`weaviate://localhost/Animal/<uuid>`). The reason is portability: a
short beacon survives export/import across namespaces (and to/from
non-NS clusters) without rewriting. Writes normalize via
`crossref.NewLocalhost(shortTarget, …)`; reads must `StripQualification`
before any inverted-index lookup, or the lookup value carries the
prefix and never matches what's on disk.

# Coverage matrix (python-client surface only)

    Lifecycle (writes through the python client)
      add/replace/delete single-target   test_single_target_add_replace_delete
      add multi-target (ReferenceToMulti) test_multi_target_refs
      replace/delete multi-target        test_multi_target_replace_and_delete
      reference_add_many multi (mixed)    test_batch_reference_insert_multi_target
      inline refs via references= kwarg   test_inline_ref_in_object_create

    Filter builders (parser → inverted-index, via the client's Filter API)
      by_ref.by_property + AND            test_filter_by_ref_chained_with_property
      by_ref.by_id                        test_filter_by_ref_chained_with_property

    Reads (client return_references serialisation + cross-node)
      return_references single + multi    test_single_target_add_replace_delete,
                                          test_multi_target_refs

Moved to Go (don't need the python-client surface):
  - batch single-target insert            test/acceptance/namespace/references_test.go
                                          (gRPC batch references each pair resolves)
  - by_ref_multi_target filter            (gRPC filter-by-ref via MultiTarget)
  - by_ref_count filter                   (gRPC by_ref_count filter)
  - nested return_references + self-ref   (gRPC nested return_references with self-ref cycle)
  - raw REST inline ref form              (create object with ref property in Properties payload)
  - storage / isolation invariants        (admin delete w/ qualified beacon, namespaced
                                          cross-NS delete, stored beacon short, namespaces
                                          stay isolated)

# Conventions

* Writes go to node 0, reads to node 1 or 2 — cross-node RAFT exercise.
* `cleanup_collections` pre-deletes at register-time so a crashed run
  doesn't poison the next.
"""

import time
import uuid
from typing import Callable, Generator, Iterator, List, Tuple

import pytest
import weaviate.classes as wvc
from weaviate import WeaviateClient
from weaviate.collections.classes.data import DataObject, DataReference
from weaviate.collections.classes.filters import Filter
from weaviate.collections.classes.grpc import QueryReference
from weaviate.collections.classes.internal import ReferenceToMulti

from . import namespace_helpers as nsh

# Ports must match docker-compose-namespaces-test.yml. Three nodes so we can
# distribute writes/reads across the cluster within a single test.
NODES: List[Tuple[int, int]] = [
    (8190, 50190),
    (8191, 50191),
    (8192, 50192),
]
ADMIN_KEY = "admin-key"

NS1 = "customer1"
NS2 = "customer2"

# Pin every test in this file to a single pytest-xdist worker. The module-scoped
# `namespaces` fixture creates one DB user per namespace; without this marker,
# every parallel worker races to create the same users, and the 409→delete→
# recreate fallback in namespace_helpers.create_user kills the prior worker's
# apikey, leaving wait_for_key polling 401 forever. Same pattern as
# test_readonly_recovery.py.
pytestmark = pytest.mark.xdist_group(name="namespace_refs")


@pytest.fixture(scope="module")
def namespaces() -> Iterator[Tuple[str, str]]:
    """Create customer1 + customer2 with one admin DB user each, yielding their keys.

    Module-scoped so the namespace+user setup is paid once per test file rather
    than per-test. Goes through the client's namespaces/users/roles APIs wrapped
    with RAFT-apply-lag retries (namespace_helpers). Cleanup is best-effort; a
    reused cluster picks up the existing namespaces via the create idempotency.
    """
    admin = nsh.open_client("admin-key", *NODES[0], skip_init_checks=False)
    try:
        nsh.create_namespace(admin, NS1)
        nsh.create_namespace(admin, NS2)
        k1 = nsh.create_user(admin, f"{NS1}:u1")
        nsh.assign_role(admin, f"{NS1}:u1", "admin")
        k2 = nsh.create_user(admin, f"{NS2}:u2")
        nsh.assign_role(admin, f"{NS2}:u2", "admin")
    finally:
        admin.close()
    nsh.wait_for_key(k1, NODES)
    nsh.wait_for_key(k2, NODES)
    yield k1, k2
    # No teardown: collections are cleaned per-test, and the namespace
    # itself is cheap to leave around — DeleteNamespace requires the
    # cleanup tick to drain, which adds noise to test timings.


@pytest.fixture
def client_for_key() -> Iterator[Callable[[str, int], WeaviateClient]]:
    """Open a client against the given node (0..2) with the given API key.

    Per-call so a single test can hit multiple nodes (write to node 0,
    read from node 2) without sharing a connection.
    """
    opened: List[WeaviateClient] = []

    def _open(key: str, node: int = 0) -> WeaviateClient:
        http_port, grpc_port = NODES[node]
        c = nsh.open_client(key, http_port, grpc_port, skip_init_checks=False)
        opened.append(c)
        return c

    try:
        yield _open
    finally:
        for c in opened:
            try:
                c.close()
            except Exception:
                pass


def _wait_for_collections_on_read_node(client: WeaviateClient, shorts: List[str]) -> None:
    """Poll until every collection is visible on the node `client` targets.

    collections.create barriers only on the writer's node; a reader on
    another node can outrun the schema apply and see "class not found".
    """
    for short in shorts:
        deadline = time.time() + 10.0
        while time.time() < deadline:
            if client.collections.exists(short):
                break
            time.sleep(0.05)
        else:
            raise AssertionError(f"collection {short!r} not visible on read node within 10s")


def _short_name(request: pytest.FixtureRequest, suffix: str = "") -> str:
    """Per-test class-name root. Short class — the namespace prefix is
    added by the server when reads happen via the qualified admin client."""
    raw = request.node.name + suffix
    s = "".join(ch for ch in raw if ch.isalnum())
    return s[0].upper() + s[1:]


@pytest.fixture
def cleanup_collections(
    namespaces: Tuple[str, str],
) -> Generator[Callable[[str], None], None, None]:
    """Pre-delete + post-delete a collection in both namespaces.

    Tests call register("Zoo") *before* the create call. Each namespace's admin
    client deletes the short name — the server qualifies it to that namespace, so
    no ':'-bearing name has to cross the client's local name validation. The
    pre-delete lets a re-run after a crashed test start clean; teardown repeats it.
    """
    k1, k2 = namespaces
    clients = [nsh.open_client(k1, *NODES[0]), nsh.open_client(k2, *NODES[0])]
    to_delete: List[str] = []

    def _delete_in_both(short: str) -> None:
        for c in clients:
            try:
                c.collections.delete(short)
            except Exception:
                pass

    def _register(short: str) -> None:
        to_delete.append(short)
        _delete_in_both(short)

    try:
        yield _register
        for short in to_delete:
            _delete_in_both(short)
    finally:
        for c in clients:
            try:
                c.close()
            except Exception:
                pass


# ---------------------------------------------------------------------------
# Single-target add / replace / delete
# ---------------------------------------------------------------------------


def test_single_target_add_replace_delete(
    request: pytest.FixtureRequest,
    namespaces: Tuple[str, str],
    client_for_key: Callable[[str, int], WeaviateClient],
    cleanup_collections: Callable[[str], None],
) -> None:
    """Single-target reference_add / replace / delete + cross-node read.

    /references add → replace → delete → re-add, then read from a
    different node and verify the link resolves. Each write must
    normalize the beacon to SHORT for portability while the existence
    check and delete-target lookup qualify against `customer1:Animal`.
    """
    k1, _ = namespaces
    zoo, animal = _short_name(request, "Zoo"), _short_name(request, "Animal")
    cleanup_collections(zoo)
    cleanup_collections(animal)

    write_client = client_for_key(k1, 0)
    read_client = client_for_key(k1, 2)

    animal_w = write_client.collections.create(
        name=animal,
        properties=[wvc.config.Property(name="name", data_type=wvc.config.DataType.TEXT)],
        vectorizer_config=wvc.config.Configure.Vectorizer.none(),
    )
    zoo_w = write_client.collections.create(
        name=zoo,
        properties=[wvc.config.Property(name="name", data_type=wvc.config.DataType.TEXT)],
        references=[wvc.config.ReferenceProperty(name="hasAnimals", target_collection=animal)],
        vectorizer_config=wvc.config.Configure.Vectorizer.none(),
    )

    _wait_for_collections_on_read_node(read_client, [animal, zoo])

    a1, a2 = uuid.uuid4(), uuid.uuid4()
    animal_w.data.insert(properties={"name": "lion"}, uuid=a1)
    animal_w.data.insert(properties={"name": "tiger"}, uuid=a2)
    zoo_id = zoo_w.data.insert(properties={"name": "z"})

    zoo_w.data.reference_add(from_uuid=zoo_id, from_property="hasAnimals", to=a1)
    zoo_w.data.reference_replace(from_uuid=zoo_id, from_property="hasAnimals", to=a2)
    zoo_w.data.reference_delete(from_uuid=zoo_id, from_property="hasAnimals", to=a2)

    obj = read_client.collections.use(zoo).query.fetch_object_by_id(
        zoo_id, return_references=QueryReference(link_on="hasAnimals")
    )
    assert obj is not None
    assert len(obj.references["hasAnimals"].objects) == 0

    # Re-add and confirm the cross-node read resolves the linked object.
    zoo_w.data.reference_add(from_uuid=zoo_id, from_property="hasAnimals", to=a1)
    obj = read_client.collections.use(zoo).query.fetch_object_by_id(
        zoo_id, return_references=QueryReference(link_on="hasAnimals", return_properties=["name"])
    )
    assert obj is not None
    refs = obj.references["hasAnimals"].objects
    assert len(refs) == 1
    assert refs[0].uuid == a1
    assert refs[0].properties["name"] == "lion"


# ---------------------------------------------------------------------------
# Multi-target refs
# ---------------------------------------------------------------------------


def test_multi_target_refs(
    request: pytest.FixtureRequest,
    namespaces: Tuple[str, str],
    client_for_key: Callable[[str, int], WeaviateClient],
    cleanup_collections: Callable[[str], None],
) -> None:
    """Multi-target reference_add (one per target class) + ref-resolve read.

    The caller's `target_collection` on ReferenceToMulti arrives short;
    the write path qualifies it for the existence check and strips
    back to short for the stored beacon (portability). Read uses
    QueryReference.MultiTarget once per target — each call needs the
    Replier to qualify the requested target class via the source's
    namespace, otherwise the multi-get misses the qualified storage.

    Companion: test_multi_target_replace_and_delete covers the rest of
    the lifecycle.
    """
    k1, _ = namespaces
    src = _short_name(request, "Src")
    alpha = _short_name(request, "Alpha")
    beta = _short_name(request, "Beta")
    for s in (src, alpha, beta):
        cleanup_collections(s)

    cw = client_for_key(k1, 0)
    cr = client_for_key(k1, 1)

    alpha_w = cw.collections.create(
        name=alpha,
        properties=[wvc.config.Property(name="name", data_type=wvc.config.DataType.TEXT)],
        vectorizer_config=wvc.config.Configure.Vectorizer.none(),
    )
    beta_w = cw.collections.create(
        name=beta,
        properties=[wvc.config.Property(name="name", data_type=wvc.config.DataType.TEXT)],
        vectorizer_config=wvc.config.Configure.Vectorizer.none(),
    )
    src_w = cw.collections.create(
        name=src,
        properties=[wvc.config.Property(name="name", data_type=wvc.config.DataType.TEXT)],
        references=[
            wvc.config.ReferenceProperty.MultiTarget(
                name="hasOther", target_collections=[alpha, beta]
            )
        ],
        vectorizer_config=wvc.config.Configure.Vectorizer.none(),
    )

    _wait_for_collections_on_read_node(cr, [alpha, beta, src])

    a_id = alpha_w.data.insert(properties={"name": "a"})
    b_id = beta_w.data.insert(properties={"name": "b"})
    src_id = src_w.data.insert(properties={"name": "s"})

    # Inline ref-add via ReferenceToMulti on both targets.
    src_w.data.reference_add(
        from_uuid=src_id,
        from_property="hasOther",
        to=ReferenceToMulti(target_collection=alpha, uuids=a_id),
    )
    src_w.data.reference_add(
        from_uuid=src_id,
        from_property="hasOther",
        to=ReferenceToMulti(target_collection=beta, uuids=b_id),
    )

    # Read from a different node and verify both targets are inlined with
    # their per-target properties.
    obj = cr.collections.use(src).query.fetch_object_by_id(
        src_id,
        return_references=QueryReference.MultiTarget(
            link_on="hasOther",
            target_collection=alpha,
            return_properties=["name"],
        ),
    )
    assert obj is not None
    alpha_refs = obj.references["hasOther"].objects
    assert len(alpha_refs) == 1
    assert alpha_refs[0].properties["name"] == "a"

    obj = cr.collections.use(src).query.fetch_object_by_id(
        src_id,
        return_references=QueryReference.MultiTarget(
            link_on="hasOther",
            target_collection=beta,
            return_properties=["name"],
        ),
    )
    assert obj is not None
    beta_refs = obj.references["hasOther"].objects
    assert len(beta_refs) == 1
    assert beta_refs[0].properties["name"] == "b"


# ---------------------------------------------------------------------------
# Filters: by_ref + chained property filter
# ---------------------------------------------------------------------------


def test_filter_by_ref_chained_with_property(
    request: pytest.FixtureRequest,
    namespaces: Tuple[str, str],
    client_for_key: Callable[[str, int], WeaviateClient],
    cleanup_collections: Callable[[str], None],
) -> None:
    """by_ref(prop).by_property(...) + AND'ed with a non-ref filter.

    The path that was broken before searcher_ref_filter.go's
    StripQualification fix: the nested search returns qualified
    ClassName, the source's `property_ref` bucket holds short
    beacons, lookup misses → 0 rows. Two variants exercised here:
    by_property AND by_ref.by_property, and by_ref.by_id.

    Companion: Go acceptance test
    `TestNamespaces_References/gRPC filter-by-ref via SingleTarget
    returns the right row on NS cluster`.
    """
    k1, _ = namespaces
    src, tgt = _short_name(request, "S"), _short_name(request, "T")
    cleanup_collections(src)
    cleanup_collections(tgt)

    cw = client_for_key(k1, 0)
    cr = client_for_key(k1, 2)

    tgt_w = cw.collections.create(
        name=tgt,
        properties=[
            wvc.config.Property(name="grade", data_type=wvc.config.DataType.INT),
            wvc.config.Property(name="text", data_type=wvc.config.DataType.TEXT),
        ],
        vectorizer_config=wvc.config.Configure.Vectorizer.none(),
    )
    src_w = cw.collections.create(
        name=src,
        properties=[wvc.config.Property(name="city", data_type=wvc.config.DataType.TEXT)],
        references=[wvc.config.ReferenceProperty(name="ref", target_collection=tgt)],
        vectorizer_config=wvc.config.Configure.Vectorizer.none(),
    )

    _wait_for_collections_on_read_node(cr, [tgt, src])

    t_low = tgt_w.data.insert(properties={"grade": 1, "text": "low"})
    t_mid = tgt_w.data.insert(properties={"grade": 5, "text": "mid"})
    t_high = tgt_w.data.insert(properties={"grade": 10, "text": "high"})

    src_w.data.insert(properties={"city": "berlin"}, references={"ref": t_low})
    src_w.data.insert(properties={"city": "berlin"}, references={"ref": t_high})
    src_w.data.insert(properties={"city": "paris"}, references={"ref": t_mid})
    src_w.data.insert(properties={"city": "paris"}, references={"ref": t_high})

    # AND: city == "berlin" AND ref.grade > 3 → only the berlin/high row.
    f = Filter.by_property("city").equal("berlin") & Filter.by_ref("ref").by_property(
        "grade"
    ).greater_than(3)
    res = cr.collections.use(src).query.fetch_objects(filters=f).objects
    assert len(res) == 1
    assert res[0].properties["city"] == "berlin"

    # by_id filter on the linked object — different code path inside the parser.
    f_id = Filter.by_ref("ref").by_id().equal(t_mid)
    res = cr.collections.use(src).query.fetch_objects(filters=f_id).objects
    assert len(res) == 1
    assert res[0].properties["city"] == "paris"


# ---------------------------------------------------------------------------
# Multi-target lifecycle: replace + delete
# ---------------------------------------------------------------------------


def test_multi_target_replace_and_delete(
    request: pytest.FixtureRequest,
    namespaces: Tuple[str, str],
    client_for_key: Callable[[str, int], WeaviateClient],
    cleanup_collections: Callable[[str], None],
) -> None:
    """Multi-target reference_replace + reference_delete.

    Lifecycle complement to test_multi_target_refs. Seed (Alpha, Beta),
    replace with one fresh Alpha (clears the list), add Beta back,
    then delete the Alpha entry. The replace/delete handlers must
    qualify each target_collection independently and match deletes on
    (target_collection, uuid) so the surviving Beta entry isn't
    collateral damage.
    """
    k1, _ = namespaces
    src = _short_name(request, "Src")
    alpha = _short_name(request, "Alpha")
    beta = _short_name(request, "Beta")
    for s in (src, alpha, beta):
        cleanup_collections(s)

    cw = client_for_key(k1, 0)
    cr = client_for_key(k1, 2)

    alpha_w = cw.collections.create(
        name=alpha,
        properties=[wvc.config.Property(name="name", data_type=wvc.config.DataType.TEXT)],
        vectorizer_config=wvc.config.Configure.Vectorizer.none(),
    )
    beta_w = cw.collections.create(
        name=beta,
        properties=[wvc.config.Property(name="name", data_type=wvc.config.DataType.TEXT)],
        vectorizer_config=wvc.config.Configure.Vectorizer.none(),
    )
    src_w = cw.collections.create(
        name=src,
        properties=[wvc.config.Property(name="name", data_type=wvc.config.DataType.TEXT)],
        references=[
            wvc.config.ReferenceProperty.MultiTarget(name="ref", target_collections=[alpha, beta])
        ],
        vectorizer_config=wvc.config.Configure.Vectorizer.none(),
    )

    _wait_for_collections_on_read_node(cr, [alpha, beta, src])

    a1, a2 = alpha_w.data.insert({"name": "a1"}), alpha_w.data.insert({"name": "a2"})
    b1, b2 = beta_w.data.insert({"name": "b1"}), beta_w.data.insert({"name": "b2"})
    src_id = src_w.data.insert(properties={"name": "s"})
    # Seed via two reference_add calls — the python client doesn't accept
    # a list of ReferenceToMulti in the initial insert payload (it tries
    # to serialize each as a single beacon and trips the cref URI parser).
    src_w.data.reference_add(
        from_uuid=src_id,
        from_property="ref",
        to=ReferenceToMulti(target_collection=alpha, uuids=a1),
    )
    src_w.data.reference_add(
        from_uuid=src_id,
        from_property="ref",
        to=ReferenceToMulti(target_collection=beta, uuids=b1),
    )

    # Replace clears the entire list and seeds it with just (alpha, a2).
    # The replace handler must qualify alpha against customer1 — a
    # regression that kept the short name in the existence check would
    # 422 here. (The python client's reference_replace only accepts a
    # single SingleReferenceInput, not a list of ReferenceToMulti, so we
    # replace with one target and then add the other back below.)
    src_w.data.reference_replace(
        from_uuid=src_id,
        from_property="ref",
        to=ReferenceToMulti(target_collection=alpha, uuids=a2),
    )
    # Add a Beta entry back so the final state has one of each target.
    src_w.data.reference_add(
        from_uuid=src_id,
        from_property="ref",
        to=ReferenceToMulti(target_collection=beta, uuids=b2),
    )

    # Delete only the Alpha entry — Beta must survive. The delete handler
    # has to match on (target_collection, uuid) and qualify alpha so the
    # stored beacon comparison hits.
    src_w.data.reference_delete(
        from_uuid=src_id,
        from_property="ref",
        to=ReferenceToMulti(target_collection=alpha, uuids=a2),
    )

    # Read from a different node and confirm only the b2 entry remains.
    obj = cr.collections.use(src).query.fetch_object_by_id(
        src_id,
        return_references=QueryReference.MultiTarget(
            link_on="ref",
            target_collection=beta,
            return_properties=["name"],
        ),
    )
    assert obj is not None
    surviving = obj.references["ref"].objects
    assert len(surviving) == 1, f"expected 1 Beta ref, got {len(surviving)}"
    assert surviving[0].properties["name"] == "b2"

    # The Alpha view should be empty after the delete.
    obj = cr.collections.use(src).query.fetch_object_by_id(
        src_id,
        return_references=QueryReference.MultiTarget(
            link_on="ref",
            target_collection=alpha,
            return_properties=["name"],
        ),
    )
    assert obj is not None
    alpha_surviving = obj.references.get("ref")
    if alpha_surviving is not None:
        assert (
            len(alpha_surviving.objects) == 0
        ), f"expected no Alpha refs after delete, got {[o.properties for o in alpha_surviving.objects]}"


# ---------------------------------------------------------------------------
# Batch reference insert (multi-target)
# ---------------------------------------------------------------------------


def test_batch_reference_insert_multi_target(
    request: pytest.FixtureRequest,
    namespaces: Tuple[str, str],
    client_for_key: Callable[[str, int], WeaviateClient],
    cleanup_collections: Callable[[str], None],
) -> None:
    """reference_add_many with DataReference.MultiTarget, mixed targets.

    Multi-target complement to test_batch_reference_insert. Odd
    indices link to A, even to B in a single batch call — the
    handler must qualify each ref's target class independently
    rather than reusing the first ref's resolution. Cross-node read
    verifies each source resolves to the labeled object on the right
    target class.
    """
    k1, _ = namespaces
    src = _short_name(request, "Src")
    a = _short_name(request, "A")
    b = _short_name(request, "B")
    for s in (src, a, b):
        cleanup_collections(s)

    cw = client_for_key(k1, 0)
    cr = client_for_key(k1, 2)

    a_w = cw.collections.create(
        name=a,
        properties=[wvc.config.Property(name="label", data_type=wvc.config.DataType.TEXT)],
        vectorizer_config=wvc.config.Configure.Vectorizer.none(),
    )
    b_w = cw.collections.create(
        name=b,
        properties=[wvc.config.Property(name="label", data_type=wvc.config.DataType.TEXT)],
        vectorizer_config=wvc.config.Configure.Vectorizer.none(),
    )
    src_w = cw.collections.create(
        name=src,
        properties=[wvc.config.Property(name="idx", data_type=wvc.config.DataType.INT)],
        references=[
            wvc.config.ReferenceProperty.MultiTarget(name="ref", target_collections=[a, b])
        ],
        vectorizer_config=wvc.config.Configure.Vectorizer.none(),
    )

    _wait_for_collections_on_read_node(cr, [a, b, src])

    N = 6
    a_uuids = [a_w.data.insert({"label": f"a-{i}"}) for i in range(N)]
    b_uuids = [b_w.data.insert({"label": f"b-{i}"}) for i in range(N)]
    src_uuids = [src_w.data.insert({"idx": i}) for i in range(N)]

    # Mixed batch: odd indices link to Alpha, even to Beta.
    refs = []
    for i in range(N):
        if i % 2 == 1:
            refs.append(
                DataReference.MultiTarget(
                    from_property="ref",
                    from_uuid=src_uuids[i],
                    to_uuid=a_uuids[i],
                    target_collection=a,
                )
            )
        else:
            refs.append(
                DataReference.MultiTarget(
                    from_property="ref",
                    from_uuid=src_uuids[i],
                    to_uuid=b_uuids[i],
                    target_collection=b,
                )
            )
    batch_ret = src_w.data.reference_add_many(refs)
    assert batch_ret.has_errors is False, f"batch errors: {batch_ret.errors}"

    # Read every odd source asking for its Alpha ref; every even
    # source asking for its Beta ref. Each must resolve to the right
    # labeled object.
    for i in range(N):
        if i % 2 == 1:
            obj = cr.collections.use(src).query.fetch_object_by_id(
                src_uuids[i],
                return_references=QueryReference.MultiTarget(
                    link_on="ref",
                    target_collection=a,
                    return_properties=["label"],
                ),
            )
            assert obj is not None
            got = obj.references["ref"].objects
            assert len(got) == 1, f"i={i}: expected 1 Alpha ref, got {len(got)}"
            assert got[0].properties["label"] == f"a-{i}"
        else:
            obj = cr.collections.use(src).query.fetch_object_by_id(
                src_uuids[i],
                return_references=QueryReference.MultiTarget(
                    link_on="ref",
                    target_collection=b,
                    return_properties=["label"],
                ),
            )
            assert obj is not None
            got = obj.references["ref"].objects
            assert len(got) == 1, f"i={i}: expected 1 Beta ref, got {len(got)}"
            assert got[0].properties["label"] == f"b-{i}"


# ---------------------------------------------------------------------------
# Inline ref in object create (Properties payload)
# ---------------------------------------------------------------------------


def test_inline_ref_in_object_create(
    request: pytest.FixtureRequest,
    namespaces: Tuple[str, str],
    client_for_key: Callable[[str, int], WeaviateClient],
    cleanup_collections: Callable[[str], None],
) -> None:
    """Refs embedded in the Properties payload via the python client's
    `references=` kwarg.

    Distinct from the raw REST beacon-in-Properties form (covered by the
    Go test `create object with ref property in Properties payload (NS
    happy path)`): the python client builds the beacon for the caller
    from a bare UUID arg, so it exercises a different client-side
    serialisation path than what a raw POST would produce.
    """
    k1, _ = namespaces
    zoo, animal = _short_name(request, "Zoo"), _short_name(request, "Animal")
    cleanup_collections(zoo)
    cleanup_collections(animal)

    cw = client_for_key(k1, 0)
    cr = client_for_key(k1, 2)

    cw.collections.create(
        name=animal,
        properties=[wvc.config.Property(name="name", data_type=wvc.config.DataType.TEXT)],
        vectorizer_config=wvc.config.Configure.Vectorizer.none(),
    )
    cw.collections.create(
        name=zoo,
        properties=[wvc.config.Property(name="name", data_type=wvc.config.DataType.TEXT)],
        references=[wvc.config.ReferenceProperty(name="hasAnimals", target_collection=animal)],
        vectorizer_config=wvc.config.Configure.Vectorizer.none(),
    )

    _wait_for_collections_on_read_node(cr, [animal, zoo])

    a_id = cw.collections.use(animal).data.insert(properties={"name": "leo"})

    # The python client wraps the UUID into a beacon for us — make sure
    # the server-side validation accepts that serialisation on NS clusters.
    zoo_id = cw.collections.use(zoo).data.insert(
        properties={"name": "zoo-from-client"},
        references={"hasAnimals": a_id},
    )

    # Cross-node read: the ref resolves to leo.
    obj = cr.collections.use(zoo).query.fetch_object_by_id(
        zoo_id,
        return_references=QueryReference(link_on="hasAnimals", return_properties=["name"]),
    )
    assert obj is not None
    refs = obj.references["hasAnimals"].objects
    assert len(refs) == 1
    assert refs[0].properties["name"] == "leo"
