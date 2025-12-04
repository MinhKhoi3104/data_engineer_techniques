import os, sys
from pyspark.sql.functions import *
from datetime import date
current_dir = os.path.dirname(__file__)
config_path = os.path.join(current_dir, '..','..')
config_path = os.path.abspath(config_path)
sys.path.insert(0, config_path)
from _001_config._00101_database_config import JDBC_URL_POSTGRES
from _002_utils._00201_utils import *

def _0201_customer_overwrite(etl_date=None):
    try:
        spark = create_spark_session("_0201_customer_overwrite")

        if etl_date is None:
            etl_date = int(date.today().strftime("%Y%m%d"))

        # Tạo schema nếu chưa tồn tại
        sql_create_schema = """CREATE SCHEMA IF NOT EXISTS prc"""
        execute_sql_ddl (
                spark = spark,
                sql_query = sql_create_schema,
                jdbc_url = JDBC_URL_POSTGRES["url"],
                properties = JDBC_URL_POSTGRES["properties"]
            )
        
        # Tạo bảng target nếu chưa tồn tại
        sql_create_target_tbl= """
            CREATE TABLE IF NOT EXISTS prc.customer(
            customer_id int4, -- ID
            customer_name VARCHAR(100), -- Tên 
            customer_birth DATE,              -- Ngày tháng năm sinh
            customer_hometown VARCHAR(150),           -- Quê quán (hoặc địa chỉ)
            customer_phone_num VARCHAR(20), -- Số điện thoại 
            customer_email VARCHAR(100),       -- Email 
            customer_source VARCHAR(100), -- tên tổ chức trích từ phía sau @ của mail
            customer_gender VARCHAR(10),              -- Giới tính (Nam, Nữ, Khác)
            is_active CHAR(1),
            etl_date DATE
            );
        """
        
        execute_sql_ddl (
                spark = spark,
                sql_query = sql_create_target_tbl,
                jdbc_url = JDBC_URL_POSTGRES["url"],
                properties = JDBC_URL_POSTGRES["properties"]
            )
        
        # Lấy dữ liệu từ stg để xử lý tại prc
        source_df = spark.read\
            .jdbc(url=JDBC_URL_POSTGRES["url"],\
            table= "(SELECT * FROM stg.customer) a",
            properties=JDBC_URL_POSTGRES["properties"])

        
        # Thực hiện ETL dữ liệu theo yêu cầu
        """Phần code phía dưới chỉ xử etl minh họa"""

        # ETL dữ liệu từ nguồn (tầng stg)
        new_target_df = \
            source_df\
                .select(
                    col("customer_id"),
                    col("full_name").alias("customer_name"),
                    col("date_of_birth").alias("customer_birth"),
                    col("hometown").alias("customer_hometown"),
                    col("phone_number").alias("customer_phone_num"),
                    col("email").alias("customer_email"),
                    expr("SUBSTRING_INDEX(SUBSTRING_INDEX(email, '@', -1), '.', 1)").alias("customer_source"),
                    col("gender").alias("customer_gender"),
                    lit("1").alias("is_active"),
                    to_date(lit(etl_date), "yyyyMMdd").alias("etl_date")
                )

        new_target_df.show(10, truncate=False)

        # Đưa dữ liệu vào target
        new_target_df.write \
                .format("jdbc") \
                .mode("overwrite") \
                .option("url", JDBC_URL_POSTGRES["url"]) \
                .option("dbtable", "prc.customer") \
                .options(**JDBC_URL_POSTGRES["properties"]) \
                .save()
        print(f"✅ Ghi dữ liệu thành công vào bảng prc.customer")
        
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
        description="job _0201_customer_overwrite"
    )
    parser.add_argument("--etl_date", type=int, help="etl_date (YYYYMMDD)")
    args = parser.parse_args()

    success = _0201_customer_overwrite(etl_date=args.etl_date)
    print(f"Job completed - Success: {success}")
    exit(0 if success else 1)
