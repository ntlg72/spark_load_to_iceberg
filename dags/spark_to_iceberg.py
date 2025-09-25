from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1),
    "retries": 1,
}

JARS_PATH = "/opt/spark/jars"

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
            # 🔗 JARs locales en la imagen
            "spark.jars": (
                f"{JARS_PATH}/scala-library-2.12.18.jar,"
                f"{JARS_PATH}/hadoop-aws-3.3.4.jar,"
                f"{JARS_PATH}/aws-java-sdk-bundle-1.12.761.jar,"
                f"{JARS_PATH}/iceberg-spark-runtime-3.5_2.12-1.5.0.jar,"
                f"{JARS_PATH}/iceberg-aws-1.5.0.jar,"
                f"{JARS_PATH}/nessie-spark-extensions-3.5_2.12-0.95.0.jar,"
                f"{JARS_PATH}/postgresql-42.7.3.jar"
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
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.sql.catalog.nessie.s3.endpoint": "http://minio:9000",
            "spark.sql.catalog.nessie.s3.path-style-access": "true",
            "spark.sql.catalog.nessie.s3.region": "us-east-1",
            "spark.sql.catalog.nessie.warehouse": "s3a://gold/",

            # 🛡️ Configuración de acceso a S3A (MinIO)
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
            "spark.hadoop.fs.s3a.access.key": "admin",
            "spark.hadoop.fs.s3a.secret.key": "password",
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
            "spark.hadoop.fs.s3a.region": "us-east-1",

            # ⏱️ Timeouts y límites
            "spark.hadoop.fs.s3a.connection.timeout": "60000",
            "spark.hadoop.fs.s3a.connection.establish.timeout": "60000",
            "spark.hadoop.fs.s3a.connection.request.timeout": "60000",
            "spark.hadoop.fs.s3a.connection.socket.timeout": "60000",
            "spark.hadoop.fs.s3a.connection.maximum": "50",
            "spark.hadoop.fs.s3a.attempts.maximum": "20",
            "spark.hadoop.fs.s3a.retry.limit": "5",

            # 🛠️ Spark master y AWS region
            "spark.master": "spark://spark-master:7077",
            "spark.submit.deployMode": "client",
            "spark.hadoop.aws.region": "us-east-1",
            "spark.driver.extraJavaOptions": "-Daws.region=us-east-1",
            "spark.executor.extraJavaOptions": "-Daws.region=us-east-1",
        },
        env_vars={
            "AWS_REGION": "us-east-1",
            "AWS_DEFAULT_REGION": "us-east-1",
        },
    )

load_to_iceberg
