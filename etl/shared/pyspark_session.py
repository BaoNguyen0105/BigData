from pyspark.sql import SparkSession, DataFrame

def get_spark_session(app_name: str = "Olist-DW-ETL") -> SparkSession:
    """Initialize and return a configured local SparkSession."""
    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.driver.memory", "4g")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark
