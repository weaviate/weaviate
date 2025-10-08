from dataclasses import dataclass, field
from typing import List, Optional
import httpx


@dataclass
class MuveraConfig:
    enabled: Optional[bool] = None
    ksim: Optional[int] = None
    dprojections: Optional[int] = None
    repetitions: Optional[int] = None


@dataclass
class Dimensionality:
    dimensions: Optional[int] = None
    count: Optional[int] = None


@dataclass
class MultiVectorConfig:
    enabled: bool
    muvera_config: MuveraConfig


@dataclass
class VectorUsage:
    name: Optional[str] = None
    vector_index_type: Optional[str] = None
    is_dynamic: Optional[bool] = None
    compression: Optional[str] = None
    vector_compression_ratio: Optional[float] = None
    bits: Optional[int] = None
    dimensionalities: List[Dimensionality] = field(default_factory=list)
    multi_vector_config: Optional[MultiVectorConfig] = None


@dataclass
class ShardUsage:
    objects_count: int
    objects_storage_bytes: int
    vector_storage_bytes: int
    index_storage_bytes: int
    full_shard_storage_bytes: int
    name: Optional[str] = None
    status: Optional[str] = None
    named_vectors: List[VectorUsage] = field(default_factory=list)


@dataclass
class CollectionUsage:
    name: Optional[str] = None
    replication_factor: Optional[int] = None
    unique_shard_count: Optional[int] = None
    shards: List[ShardUsage] = field(default_factory=list)


@dataclass
class BackupUsage:
    id: Optional[str] = None
    completion_time: Optional[str] = None
    size_in_gib: Optional[float] = None
    type: Optional[str] = None
    collections: List[str] = field(default_factory=list)


@dataclass
class ObjectUsage:
    count: Optional[int] = None
    storage_bytes: Optional[int] = None


@dataclass
class Report:
    version: Optional[str] = None
    node: Optional[str] = None
    collections: List[CollectionUsage] = field(default_factory=list)
    backups: List[BackupUsage] = field(default_factory=list)
    collecting_time: Optional[str] = None
    schema: Optional[dict] = None  # models.Schema is not defined, so use dict


# Helper functions to parse nested structures


def parse_dimensionality(data):
    if data is None:
        return None
    return Dimensionality(dimensions=data.get("dimensionality"), count=data.get("count"))


def parse_vector_usage(data):
    if data is None:
        return None
    return VectorUsage(
        name=data.get("name"),
        vector_index_type=data.get("vector_index_type"),
        is_dynamic=data.get("is_dynamic"),
        compression=data.get("compression"),
        vector_compression_ratio=data.get("vector_compression_ratio"),
        bits=data.get("bits"),
        dimensionalities=[
            d
            for d in (parse_dimensionality(dd) for dd in data.get("dimensionalities", []))
            if d is not None
        ],
        multi_vector_config=(
            MultiVectorConfig(
                enabled=data["multi_vector_config"]["enabled"],
                muvera_config=(
                    MuveraConfig(
                        enabled=data["multi_vector_config"]["muvera_config"]["enabled"],
                        ksim=data["multi_vector_config"]["muvera_config"]["ksim"],
                        dprojections=data["multi_vector_config"]["muvera_config"]["dprojections"],
                        repetitions=data["multi_vector_config"]["muvera_config"]["repetitions"],
                    )
                    if "muvera_config" in data["multi_vector_config"]
                    else None
                ),
            )
        ),
    )


def parse_shard_usage(data):
    if data is None:
        return None
    return ShardUsage(
        name=data.get("name"),
        status=data.get("status"),
        objects_count=data.get("objects_count"),
        objects_storage_bytes=data.get("objects_storage_bytes", 0),
        vector_storage_bytes=data.get("vector_storage_bytes", 0),
        index_storage_bytes=data.get("index_storage_bytes", 0),
        full_shard_storage_bytes=data.get("full_shard_storage_bytes", 0),
        named_vectors=[
            v
            for v in (parse_vector_usage(vv) for vv in data.get("named_vectors", []))
            if v is not None
        ],
    )


def parse_collection_usage(data):
    if data is None:
        return None
    return CollectionUsage(
        name=data.get("name"),
        replication_factor=data.get("replication_factor"),
        unique_shard_count=data.get("unique_shard_count"),
        shards=[
            s for s in (parse_shard_usage(ss) for ss in data.get("shards", [])) if s is not None
        ],
    )


def parse_backup_usage(data):
    if data is None:
        return None
    return BackupUsage(
        id=data.get("id"),
        completion_time=data.get("completion_time"),
        size_in_gib=data.get("size_in_gib"),
        type=data.get("type"),
        collections=data.get("collections", []),
    )


def parse_report(data):
    if data is None:
        return None
    return Report(
        version=data.get("version"),
        node=data.get("node"),
        collections=[
            c
            for c in (parse_collection_usage(cc) for cc in data.get("collections", []))
            if c is not None
        ],
        backups=[
            b for b in (parse_backup_usage(bb) for bb in data.get("backups", [])) if b is not None
        ],
        collecting_time=data.get("collecting_time"),
        schema=data.get("schema"),
    )


def get_debug_usage() -> Report:
    try:
        with httpx.Client() as client:
            response = client.get(
                "http://localhost:6060/debug/usage", params={"exactObjectCount": "true"}, timeout=30
            )
            response.raise_for_status()
            data = response.json()
        return parse_report(data)
    except httpx.HTTPStatusError as exc:
        print(f"HTTP error: {exc.response.status_code} - {exc.response.text}")
        raise exc
    except httpx.RequestError as exc:
        print(f"Request error: {exc}")
        raise exc


def get_debug_usage_for_collection(collection: str) -> CollectionUsage:
    report = get_debug_usage()
    for col in report.collections:
        if col.name == collection:
            return col
    raise ValueError(f"Collection {collection} not found in debug usage report")
