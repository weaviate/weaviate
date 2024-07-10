import weaviate.classes as wvc

from .conftest import CollectionFactory

DATE0 = '2023-01-01T00:00:00Z'
DATE1 = '2023-01-01T00:00:00Z'
DATE2 = '2023-01-02T00:00:00Z'

def test_near_text_group_by(collection_factory: CollectionFactory) -> None:
    collection = collection_factory(
        vectorizer_config=wvc.config.Configure.Vectorizer.text2vec_contextionary(vectorize_collection_name=False),
        properties=[
            wvc.config.Property(
                name="text",
                data_type=wvc.config.DataType.TEXT,
            ),
            wvc.config.Property(
                name="texts",
                data_type=wvc.config.DataType.TEXT_ARRAY,
            ),
            wvc.config.Property(
                name="int",
                data_type=wvc.config.DataType.INT,
                skip_vectorization=True,
            ),
            wvc.config.Property(
                name="ints",
                data_type=wvc.config.DataType.INT_ARRAY,
                skip_vectorization=True,
            ),
            wvc.config.Property(
                name="number",
                data_type=wvc.config.DataType.NUMBER,
                skip_vectorization=True,
            ),
            wvc.config.Property(
                name="numbers",
                data_type=wvc.config.DataType.NUMBER_ARRAY,
                skip_vectorization=True,
            ),
            wvc.config.Property(
                name="date",
                data_type=wvc.config.DataType.DATE,
                skip_vectorization=True,
            ),
            wvc.config.Property(
                name="dates",
                data_type=wvc.config.DataType.DATE_ARRAY,
                skip_vectorization=True,
            ),
            wvc.config.Property(
                name="boolean",
                data_type=wvc.config.DataType.BOOL,
                skip_vectorization=True,
            ),
            wvc.config.Property(
                name="booleans",
                data_type=wvc.config.DataType.BOOL_ARRAY,
                skip_vectorization=True,
            ),
        ]
    )
    collection.data.insert_many([{
        "text": "test",
        "texts": ['tests', 'tests'],
        "int": 1,
        "ints": [1, 2],
        "number": 1.0,
        "numbers": [1.0, 2.0],
        "date": DATE0,
        "dates": [DATE1, DATE2],
        "boolean": True,
        "booleans": [True, False],
    } for _ in range(100)])
    result = collection.aggregate.near_text(
        query="test",
        group_by=wvc.aggregate.GroupByAggregate(prop="text"),
        certainty=0.5
    )
    assert len(result.groups) == 1
    assert result.groups[0].total_count == 100