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
# ==============================
def ensure_bucket_exists(endpoint, access_key, secret_key, bucket_name="gold"):
    logger.info(f"🔍 Verificando bucket `{bucket_name}` en MinIO...")
    s3 = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name="us-east-1",
    )
    buckets = [b["Name"] for b in s3.list_buckets()["Buckets"]]
    if bucket_name not in buckets:
        s3.create_bucket(Bucket=bucket_name)
        logger.info(f"🆕 Bucket `{bucket_name}` creado en MinIO")
    else:
        logger.info(f"✅ Bucket `{bucket_name}` ya existe en MinIO")

# ==============================
# Validar tabla Iceberg
# ==============================
def drop_table_if_exists(spark, table_name="nessie.gold.yellow_tripdata"):
    try:
        namespace = table_name.split(".")[1]
        table = table_name.split(".")[2]
        tables = [t.name for t in spark.sql(f"SHOW TABLES IN {namespace}").collect()]
        if table in tables:
            logger.info(f"⚠️ Tabla {table_name} existe pero archivos pueden estar corruptos. Eliminando...")
            spark.sql(f"DROP TABLE {table_name}")
            logger.info(f"✅ Tabla {table_name} eliminada")
        else:
            logger.info(f"✅ Tabla {table_name} no existe, se puede crear")
    except Exception as e:
        logger.warning(f"❌ No se pudo verificar la tabla: {e}")

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
        df.writeTo("nessie.gold.yellow_tripdata").createOrReplace()  # o .append() según tu lógica

        logger.info("🎉 Escritura completada")
    except Exception as e:
        logger.error(f"❌ Error en escritura: {e}")
        raise

# ==============================
# Main
# ==============================
def main():
    spark = SparkSession.builder.appName("LoadToIceberg").getOrCreate()
    logger.info("🚀 SparkSession iniciada")

    try:
        # Validar bucket en MinIO
        ensure_bucket_exists(
            endpoint="http://minio:9000",
            access_key="admin",
            secret_key="password"
        )

        # ETL
        df = read_data(spark)

        # Validar y eliminar tabla Iceberg si existía
        drop_table_if_exists(spark, "nessie.gold.yellow_tripdata")

        # Escritura
        write_to_iceberg(spark, df)

    finally:
        spark.stop()
        logger.info("🛑 SparkSession detenida")

if __name__ == "__main__":
    main()
