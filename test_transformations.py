from pyspark.sql import Row
from transformations import add_total_column, filter_expensive, normalize_category


class TestTransformationsPass:
    def test_add_total_column_values(self):
        df = spark.createDataFrame(
            [Row(id=1, price=10.0, qty=2),
             Row(id=2, price=5.5, qty=4)]
        )
        result = add_total_column(df).orderBy("id")
        rows = result.select("id", "total").collect()
        assert rows[0]["total"] == 20.0
        assert rows[1]["total"] == 22.0

    def test_filter_expensive_keeps_high_price(self):
        df = spark.createDataFrame(
            [Row(id=1, price=10.0),
             Row(id=2, price=4.0)]
        )
        result = filter_expensive(df, min_price=5.0)
        ids = [r["id"] for r in result.collect()]
        assert ids == [1]

    def test_normalize_category_lower_trim(self):
        df = spark.createDataFrame(
            [Row(id=1, category="  Toys "),
             Row(id=2, category="BOOKS")]
        )
        result = normalize_category(df).orderBy("id")
        rows = result.select("id", "category_norm").collect()
        assert rows[0]["category_norm"] == "toys"
        assert rows[1]["category_norm"] == "books"


class TestTransformationsFail:
    def test_wrong_total_value(self):
        df = spark.createDataFrame(
            [Row(id=1, price=10.0, qty=2)]
        )
        result = add_total_column(df)
        row = result.first()
        # 10 * 2 = 20, so this assertion is wrong and should FAIL
        assert row["total"] == 999

    def test_filter_expensive_wrong_ids(self):
        df = spark.createDataFrame(
            [Row(id=1, price=10.0),
             Row(id=2, price=4.0)]
        )
        result = filter_expensive(df, min_price=5.0)
        ids = [r["id"] for r in result.collect()]
        # Only id=1 should remain, so this should FAIL
        assert ids == [2]

    def test_normalize_category_wrong_case(self):
        df = spark.createDataFrame(
            [Row(id=1, category="  Toys ")]
        )
        result = normalize_category(df)
        row = result.first()
        # Function lowercases to "toys", so this should FAIL
        assert row["category_norm"] == "Toys"

    def test_missing_column_should_exist(self):
        df = spark.createDataFrame(
            [Row(id=1, price=10.0, qty=2)]
        )
        result = add_total_column(df)
        cols = result.columns
        # There is no such column, so this should FAIL
        assert "non_existing_col" in cols

