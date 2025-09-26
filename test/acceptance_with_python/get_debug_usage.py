from dataclasses import dataclass, field
from typing import List, Optional
import httpx


@dataclass
class Dimensionality:
    dimensions: Optional[int] = None
    count: Optional[int] = None


@dataclass
class VectorUsage:
    name: Optional[str] = None
    vector_index_type: Optional[str] = None
    is_dynamic: Optional[bool] = None
    compression: Optional[str] = None
    vector_compression_ratio: Optional[float] = None
    bits: Optional[int] = None
    dimensionalities: List[Dimensionality] = field(default_factory=list)


@dataclass
class ShardUsage:
    name: Optional[str] = None
    status: Optional[str] = None
    objects_count: Optional[int] = None
    objects_storage_bytes: Optional[int] = None
    vector_storage_bytes: Optional[int] = None
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
    return Dimensionality(dimensions=data.get("dimensionality"), count=data.get("count"))


def parse_vector_usage(data):
    return VectorUsage(
        name=data.get("name"),
        vector_index_type=data.get("vector_index_type"),
        is_dynamic=data.get("is_dynamic"),
        compression=data.get("compression"),
        vector_compression_ratio=data.get("vector_compression_ratio"),
        bits=data.get("bits"),
        dimensionalities=[parse_dimensionality(d) for d in data.get("dimensionalities", [])],
    )


def parse_shard_usage(data):
    return ShardUsage(
        name=data.get("name"),
        status=data.get("status"),
        objects_count=data.get("objects_count"),
        objects_storage_bytes=data.get("objects_storage_bytes"),
        vector_storage_bytes=data.get("vector_storage_bytes"),
        named_vectors=[parse_vector_usage(v) for v in data.get("named_vectors", [])],
    )


def parse_collection_usage(data):
    return CollectionUsage(
        name=data.get("name"),
        replication_factor=data.get("replication_factor"),
        unique_shard_count=data.get("unique_shard_count"),
        shards=[parse_shard_usage(s) for s in data.get("shards", [])],
    )


def parse_backup_usage(data):
    return BackupUsage(
        id=data.get("id"),
        completion_time=data.get("completion_time"),
        size_in_gib=data.get("size_in_gib"),
        type=data.get("type"),
        collections=data.get("collections", []),
    )


def parse_report(data):
    return Report(
        version=data.get("version"),
        node=data.get("node"),
        collections=[parse_collection_usage(c) for c in data.get("collections", [])],
        backups=[parse_backup_usage(b) for b in data.get("backups", [])],
        collecting_time=data.get("collecting_time"),
        schema=data.get("schema"),
    )


def get_debug_usage() -> Report:
    with httpx.Client() as client:
        response = client.get(
            "http://localhost:6060/debug/usage", params={"exactObjectCount": "true"}, timeout=30
        )
        response.raise_for_status()
        data = response.json()
    return parse_report(data)


def get_debug_usage_for_collection(collection: str) -> CollectionUsage:
    report = get_debug_usage()
    for col in report.collections:
        if col.name == collection:
            return col
    raise ValueError(f"Collection {collection} not found in debug usage report")
