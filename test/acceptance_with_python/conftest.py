from typing import Any, Optional, List, Generator, Protocol, Type, Dict, Tuple, Union, Callable

import pytest
from _pytest.fixtures import SubRequest

import weaviate
from weaviate.collections import Collection
from weaviate.collections.classes.config import (
    Property,
    _VectorizerConfigCreate,
    _InvertedIndexConfigCreate,
    _ReferencePropertyBase,
    _GenerativeProvider,
    _ReplicationConfigCreate,
    _MultiTenancyConfigCreate,
    _VectorIndexConfigCreate,
    _RerankerProvider,
)
from weaviate.collections.classes.types import Properties
from weaviate.config import AdditionalConfig

from weaviate.collections.classes.config_named_vectors import _NamedVectorConfigCreate
import weaviate.classes as wvc


class CollectionFactory(Protocol):
    """Typing for fixture."""

    def __call__(
        self,
        name: str = "",
        properties: Optional[List[Property]] = None,
        references: Optional[List[_ReferencePropertyBase]] = None,
        vectorizer_config: Optional[
            Union[_VectorizerConfigCreate, List[_NamedVectorConfigCreate]]
        ] = None,
        inverted_index_config: Optional[_InvertedIndexConfigCreate] = None,
        multi_tenancy_config: Optional[_MultiTenancyConfigCreate] = None,
        generative_config: Optional[_GenerativeProvider] = None,
        headers: Optional[Dict[str, str]] = None,
        ports: Tuple[int, int] = (8080, 50051),
        data_model_properties: Optional[Type[Properties]] = None,
        data_model_refs: Optional[Type[Properties]] = None,
        replication_config: Optional[_ReplicationConfigCreate] = None,
        vector_index_config: Optional[_VectorIndexConfigCreate] = None,
        description: Optional[str] = None,
        reranker_config: Optional[_RerankerProvider] = None,
    ) -> Collection[Any, Any]:
        """Typing for fixture."""
        ...


@pytest.fixture
def weaviate_client() -> Callable[[int, int], weaviate.WeaviateClient]:
    def connect(http_port: int = 8080, grpc_port: int = 50051) -> weaviate.WeaviateClient:
        return weaviate.connect_to_local(
            port=http_port,
            grpc_port=grpc_port,
            additional_config=AdditionalConfig(timeout=(60, 120)),  # for image tests
        )

    return connect


@pytest.fixture
def collection_factory(request: SubRequest) -> Generator[CollectionFactory, None, None]:
    name_fixture: Optional[str] = None
    client_fixture: Optional[weaviate.WeaviateClient] = None

    def _factory(
        name: str = "",
        properties: Optional[List[Property]] = None,
        references: Optional[List[_ReferencePropertyBase]] = None,
        vectorizer_config: Optional[
            Union[_VectorizerConfigCreate, List[_NamedVectorConfigCreate]]
        ] = None,
        inverted_index_config: Optional[_InvertedIndexConfigCreate] = None,
        multi_tenancy_config: Optional[_MultiTenancyConfigCreate] = None,
        generative_config: Optional[_GenerativeProvider] = None,
        headers: Optional[Dict[str, str]] = None,
        ports: Tuple[int, int] = (8080, 50051),
        data_model_properties: Optional[Type[Properties]] = None,
        data_model_refs: Optional[Type[Properties]] = None,
        replication_config: Optional[_ReplicationConfigCreate] = None,
        vector_index_config: Optional[_VectorIndexConfigCreate] = None,
        description: Optional[str] = None,
        reranker_config: Optional[_RerankerProvider] = None,
    ) -> Collection[Any, Any]:
        nonlocal client_fixture, name_fixture
        name_fixture = _sanitize_collection_name(request.node.name) + name
        client_fixture = weaviate.connect_to_local(
            headers=headers,
            grpc_port=ports[1],
            port=ports[0],
            additional_config=AdditionalConfig(timeout=(60, 120)),  # for image tests
        )
        client_fixture.collections.delete(name_fixture)

        collection: Collection[Any, Any] = client_fixture.collections.create(
            name=name_fixture,
            description=description,
            vectorizer_config=vectorizer_config,
            properties=properties,
            references=references,
            inverted_index_config=inverted_index_config,
            multi_tenancy_config=multi_tenancy_config,
            generative_config=generative_config,
            data_model_properties=data_model_properties,
            data_model_references=data_model_refs,
            replication_config=replication_config,
            vector_index_config=vector_index_config,
            reranker_config=reranker_config,
        )
        return collection

    try:
        yield _factory
    finally:
        if client_fixture is not None and name_fixture is not None:
            client_fixture.collections.delete(name_fixture)
            client_fixture.close()


class NamedCollection(Protocol):
    """Typing for fixture."""

    def __call__(self, name: str = "", props: Optional[List[str]] = None) -> Collection:
        """Typing for fixture."""
        ...


@pytest.fixture
def named_collection(
    collection_factory: CollectionFactory,
) -> Generator[NamedCollection, None, None]:
    def _factory(name: str = "", props: Optional[List[str]] = None) -> Collection:
        if props is None:
            props = ["title1", "title2", "title3"]

        properties = [
            Property(name=prop, data_type=wvc.config.DataType.TEXT, vectorize_property_name=False)
            for prop in props
        ]
        named_vectors = [
            wvc.config.Configure.NamedVectors.text2vec_contextionary(
                name=prop.name,
                source_properties=[prop.name],
                vectorize_collection_name=False,
            )
            for prop in properties
        ]

        collection = collection_factory(
            name,
            properties=properties,
            vectorizer_config=named_vectors,
        )

        return collection

    yield _factory


def _sanitize_collection_name(name: str) -> str:
    name = (
        name.replace("[", "")
        .replace("]", "")
        .replace("-", "")
        .replace(" ", "")
        .replace(".", "")
        .replace("{", "")
        .replace("}", "")
    )
    return name[0].upper() + name[1:]
