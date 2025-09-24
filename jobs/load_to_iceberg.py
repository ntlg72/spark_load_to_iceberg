import logging
from pyspark.sql import SparkSession
import boto3

# ==============================
# Logging
# ==============================
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ==============================
# Validar bucket en MinIO
# (opcional, lo puedes mover a un init script)
# ==============================
def ensure_bucket_exists(endpoint, access_key, secret_key):
    logger.info("🔍 Verificando bucket `gold` en MinIO...")
    s3 = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name="us-east-1",
    )
    buckets = [b["Name"] for b in s3.list_buckets()["Buckets"]]
    if "gold" not in buckets:
        s3.create_bucket(Bucket="gold")
        logger.info("🆕 Bucket `gold` creado en MinIO")
    else:
        logger.info("✅ Bucket `gold` ya existe en MinIO")

# ==============================
# Lectura de datos
# ==============================
def read_data(spark):
    try:
        logger.info("📥 Leyendo parquet desde Bronze...")
        df = spark.read.parquet("s3a://bronze/yellow_tripdata_2025-01.parquet")
        logger.info(f"✔️ Datos leídos: {df.count()} registros")
        df.printSchema()
        return df
    except Exception as e:
        logger.error(f"❌ Error en lectura: {e}")
        raise

# ==============================
# Escritura en Iceberg
# ==============================
def write_to_iceberg(spark, df):
    try:
        logger.info("🔧 Creando namespace si no existe...")
        spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.gold")

        logger.info("💾 Escribiendo en Iceberg (nessie.gold.yellow_tripdata)...")
        (
            df.writeTo("nessie.gold.yellow_tripdata")
            .createOrReplace()  # o .append() según tu lógica
        )

        logger.info("🎉 Escritura completada")
    except Exception as e:
        logger.error(f"❌ Error en escritura: {e}")
        raise

# ==============================
# Main
# ==============================
def main():
    # SparkSession ya viene configurada desde el DAG
    spark = SparkSession.builder.appName("LoadToIceberg").getOrCreate()
    logger.info("🚀 SparkSession iniciada")

    try:
        # (Opcional) validación del bucket
        ensure_bucket_exists(
            endpoint="http://minio:9000", 
            access_key="admin", 
            secret_key="password"
        )

        # ETL
        df = read_data(spark)
        write_to_iceberg(spark, df)

    finally:
        spark.stop()
        logger.info("🛑 SparkSession detenida")

if __name__ == "__main__":
    main()
