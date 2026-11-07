def test_serverless_connection(spark):
    """Proves the environment running the tests can reach Databricks serverless compute."""
    assert spark.sql("SELECT 1 AS one").first().one == 1
