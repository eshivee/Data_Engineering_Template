
from pyspark.sql import Row

def test_silver_transformation_logic(spark):
    # Dummy dataset
    data = [
        Row(id=1, amount=100),
        Row(id=None, amount=200),
        Row(id=1, amount=100),
    ]

    df = spark.createDataFrame(data)

    # Example transformation logic (deduplicate + remove null ids)
    result = df.dropDuplicates().filter("id IS NOT NULL")

    assert result.count() == 1
