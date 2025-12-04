import os, sys
current_dir = os.path.dirname(__file__)
config_path = os.path.join(current_dir, '..')
config_path = os.path.abspath(config_path)
sys.path.insert(0, config_path)
from pyspark.sql import SparkSession
from _001_config._00101_database_config import JDBC_URL_POSTGRES,PG_JDBC_JAR_PATH

def execute_sql_ddl(spark, sql_query, jdbc_url, properties):
    """Thực thi câu lệnh SQL DDL/DML trực tiếp lên database qua kết nối JDBC."""
    
    jvm = spark._jvm
    
    try:
        DriverManager = jvm.java.sql.DriverManager
        connection = DriverManager.getConnection(
            jdbc_url, 
            properties["user"], 
            properties["password"]
        )
        statement = connection.createStatement()
        statement.execute(sql_query)
        return True
        
    except Exception as e:
        print(f"❌ Error executing SQL '{sql_query}': {e}")
        return False
        
    finally:
        if 'connection' in locals() and connection is not None:
            connection.close()

def create_spark_session(appName):
    spark = SparkSession.builder \
                .appName(appName) \
                .config("spark.driver.extraClassPath", PG_JDBC_JAR_PATH) \
                .getOrCreate()
    return spark

# Tạo spark session phục vụ cho sử dụng iceberd
def create_spark_session_iceberg(appName):
    import os
    
    # Tạo warehouse path để lưu data files (Parquet/ORC) trên local
    # Data files vẫn cần lưu trên filesystem
    current_dir = os.path.dirname(os.path.abspath(__file__))
    iceberg_warehouse = os.path.join(current_dir, "..", "iceberg-warehouse")
    os.makedirs(iceberg_warehouse, exist_ok=True)
    
    spark = (
        SparkSession.builder
        .appName(appName)
        .config("spark.driver.extraClassPath", PG_JDBC_JAR_PATH)
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.jdbciceberg", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.jdbciceberg.catalog-impl", "org.apache.iceberg.jdbc.JdbcCatalog")
        .config("spark.sql.catalog.jdbciceberg.uri", JDBC_URL_POSTGRES["url"])
        .config("spark.sql.catalog.jdbciceberg.jdbc.user", JDBC_URL_POSTGRES["properties"]["user"])
        .config("spark.sql.catalog.jdbciceberg.jdbc.password", JDBC_URL_POSTGRES["properties"]["password"])
        .config("spark.sql.catalog.jdbciceberg.warehouse", f"file://{iceberg_warehouse}")   
        .getOrCreate()
    )
    return spark