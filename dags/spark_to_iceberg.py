from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1),
    "retries": 1,
}

with DAG(
    dag_id="load_to_iceberg_dag",
    default_args=default_args,
    description="Carga datos Parquet desde Bronze en MinIO hacia Iceberg (Gold) con Nessie",
    schedule_interval=None,
    catchup=False,
    tags=["spark", "iceberg", "nessie", "minio"],
) as dag:

    load_to_iceberg = SparkSubmitOperator(
        task_id="spark_load_to_iceberg",
        application="/opt/airflow/jobs/load_to_iceberg.py",
        conn_id="spark_default",
        verbose=True,
        conf={
            # 🔗 JARs para PostgreSQL, Iceberg, Nessie, AWS SDK y Hadoop AWS
            "spark.jars.packages": (
                "org.postgresql:postgresql:42.7.3,"
                "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,"
                "org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.77.1,"
                "software.amazon.awssdk:bundle:2.24.8,"
                "software.amazon.awssdk:url-connection-client:2.24.8,"
                "org.apache.hadoop:hadoop-aws:3.2.0,"
                "com.amazonaws:aws-java-sdk-bundle:1.11.534"
            ),

            # 🧠 Extensiones de Iceberg y Nessie
            "spark.sql.extensions": (
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,"
                "org.projectnessie.spark.extensions.NessieSparkSessionExtensions"
            ),

            # 🗂️ Configuración de Nessie como catálogo Iceberg
            "spark.sql.catalog.nessie": "org.apache.iceberg.spark.SparkCatalog",
            "spark.sql.catalog.nessie.uri": "http://nessie:19120/api/v1",
            "spark.sql.catalog.nessie.ref": "main",
            "spark.sql.catalog.nessie.authentication.type": "NONE",
            "spark.sql.catalog.nessie.catalog-impl": "org.apache.iceberg.nessie.NessieCatalog",
            "spark.sql.catalog.nessie.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
            "spark.sql.catalog.nessie.s3.endpoint": "http://minio:9000",
            "spark.sql.catalog.nessie.warehouse": "s3://gold/",

            # 🛡️ Configuración de acceso a S3A (MinIO)
            "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
            "spark.hadoop.fs.s3a.access.key": "admin",
            "spark.hadoop.fs.s3a.secret.key": "password",
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.aws.credentials.provider":
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",

            # ⏱️ Timeouts y límites en milisegundos
            "spark.hadoop.fs.s3a.connection.timeout": "60000",
            "spark.hadoop.fs.s3a.connection.establish.timeout": "60000",
            "spark.hadoop.fs.s3a.connection.maximum": "50",
            "spark.hadoop.fs.s3a.attempts.maximum": "20",
            "spark.hadoop.fs.s3a.retry.limit": "5",
        },
    )

    load_to_iceberg
