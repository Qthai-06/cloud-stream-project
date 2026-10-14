"""Smoke test for the local Spark and Redpanda Kafka connection."""

from pyspark.sql import SparkSession


KAFKA_CONNECTOR_PACKAGE = (
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"
)


def create_spark_session() -> SparkSession:
    """Create a small local Spark session with the Kafka connector enabled."""
    return (
        SparkSession.builder.appName("SmokeTest-Redpanda-Spark")
        .master("local[2]")
        .config("spark.jars.packages", KAFKA_CONNECTOR_PACKAGE)
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )


def main() -> None:
    """Load the Kafka source to verify Spark can initialize the connector."""
    print(">>> Dang khoi tao Spark Session voi Kafka Connector...")
    spark = create_spark_session()

    try:
        spark.sparkContext.setLogLevel("WARN")
        print(f"Spark Version: {spark.version} - Khoi tao thanh cong!")
        print(">>> Thu ket noi toi Redpanda tai localhost:9092...")

        (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", "localhost:9092")
            .option("subscribe", "test-topic")
            .option("startingOffsets", "latest")
            .load()
        )
    except Exception as error:
        raise RuntimeError(
            "Khong the khoi tao Kafka source tai localhost:9092. "
            "Hay dam bao Redpanda dang chay va topic test-topic ton tai."
        ) from error
    finally:
        spark.stop()

    print("Ket noi Spark <-> Redpanda thanh cong! Pipeline san sang nhan su kien.")


if __name__ == "__main__":
    main()
