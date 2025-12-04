import os, sys
from pyspark.sql.functions import *
from datetime import date
current_dir = os.path.dirname(__file__)
config_path = os.path.join(current_dir, '..','..')
config_path = os.path.abspath(config_path)
sys.path.insert(0, config_path)
from _001_config._00101_database_config import JDBC_URL_POSTGRES
from _002_utils._00201_utils import *

def _0101_customer_overwrite(etl_date=None):
    try:
        spark = create_spark_session("_0101_customer_overwrite")

        if etl_date is None:
            etl_date = int(date.today().strftime("%Y%m%d"))
        
        # Đọc dữ liệu từ nguồn (raw data)
        source_df =  spark.read\
            .jdbc(url=JDBC_URL_POSTGRES["url"],\
            table= "(SELECT * FROM e_commerce.customer) a",
            properties=JDBC_URL_POSTGRES["properties"])
        
        # Thêm etl_date để checking date xử lý dữ liệu
        source_df =  source_df.withColumn("etl_date", to_date(lit(etl_date), "yyyyMMdd"))
        
        # Tại tầng stg dữ liệu được đẩy trực tiếp về mà không xử lý
        sql_create_schema = """CREATE SCHEMA IF NOT EXISTS stg"""
        execute_sql_ddl (
                spark = spark,
                sql_query = sql_create_schema,
                jdbc_url = JDBC_URL_POSTGRES["url"],
                properties = JDBC_URL_POSTGRES["properties"]
            )
        
        # Tạo bảng target nếu chưa tồn tại
        sql_create_target_tbl= """
            CREATE TABLE IF NOT EXISTS stg.customer(
            customer_id int4,
            full_name VARCHAR(100), -- Tên đầy đủ
            date_of_birth DATE,              -- Ngày tháng năm sinh
            hometown VARCHAR(150),           -- Quê quán (hoặc địa chỉ)
            phone_number VARCHAR(20), -- Số điện thoại 
            email VARCHAR(100),       -- Email 
            gender VARCHAR(10),              -- Giới tính (Nam, Nữ, Khác)
            etl_date DATE
            );
        """
        
        execute_sql_ddl (
                spark = spark,
                sql_query = sql_create_target_tbl,
                jdbc_url = JDBC_URL_POSTGRES["url"],
                properties = JDBC_URL_POSTGRES["properties"]
            )
        
        # Đưa dữ liệu vào target
        source_df.write \
                .format("jdbc") \
                .mode("overwrite") \
                .option("url", JDBC_URL_POSTGRES["url"]) \
                .option("dbtable", "stg.customer") \
                .options(**JDBC_URL_POSTGRES["properties"]) \
                .save()
        print(f"✅ Ghi dữ liệu thành công vào bảng stg.customer")

        return True

    except Exception as e:
        print(f"ERROR: {e}")
        return False

    finally:
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="job _0101_customer_overwrite"
    )
    parser.add_argument("--etl_date", type=int, help="etl_date (YYYYMMDD)")
    args = parser.parse_args()

    success = _0101_customer_overwrite(etl_date=args.etl_date)
    print(f"Job completed - Success: {success}")
    exit(0 if success else 1)


