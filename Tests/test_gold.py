
from pyspark.sql import Row
from pyspark.sql.functions import sum as spark_sum

def test_gold_aggregation_logic(spark):
    data = [
        Row(category="A", amount=100),
        Row(category="A", amount=200),
        Row(category="B", amount=300),
    ]

    df = spark.createDataFrame(data)

    result = (
        df.groupBy("category")
          .agg(spark_sum("amount").alias("total_amount"))
    )

    result_dict = {row["category"]: row["total_amount"] for row in result.collect()}

    assert result_dict["A"] == 300
    assert result_dict["B"] == 300
