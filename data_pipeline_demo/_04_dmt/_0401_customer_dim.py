import os, sys
from pyspark.sql.functions import *
from datetime import date
current_dir = os.path.dirname(__file__)
config_path = os.path.join(current_dir, '..','..')
config_path = os.path.abspath(config_path)
sys.path.insert(0, config_path)
from _001_config._00101_database_config import JDBC_URL_POSTGRES
from _002_utils._00201_utils import *

def _0401_customer_dim(etl_date=None):
    try:
        # Tại tầng này sẽ ứng dụng Iceberg
        spark = create_spark_session_iceberg("_0401_customer_dim")

        if etl_date is None:
            etl_date = int(date.today().strftime("%Y%m%d"))

        # Tạo namespace trong iceberg
        spark.sql("CREATE NAMESPACE IF NOT EXISTS dmt")
        
        # Tạo bảng target nếu chưa tồn tại trong iceberg
        spark.sql("""drop table if exists jdbciceberg.dmt.customer""")
        spark.sql("""
            CREATE TABLE IF NOT EXISTS jdbciceberg.cur.customer(
            customer_id int, -- ID
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
        """)

        # Tạo schema nếu chưa tồn tại trong postgreSQL
        sql_create_schema = """CREATE SCHEMA IF NOT EXISTS dmt"""
        execute_sql_ddl (
                spark = spark,
                sql_query = sql_create_schema,
                jdbc_url = JDBC_URL_POSTGRES["url"],
                properties = JDBC_URL_POSTGRES["properties"]
            )
        
        # Tạo bảng target nếu chưa tồn tại trong postgreSQL
        sql_create_target_tbl= """
            CREATE TABLE IF NOT EXISTS dmt.customer_dim(
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

        # Lấy dữ liệu source từ cur để xử lý dmt
        source_df = spark.read.format("iceberg").load("jdbciceberg.cur.customer")

        # Lấy dữ liệu target tầng dmt
        target_df = spark.read.format("iceberg").load("jdbciceberg.cur.customer")

        # Thực hiện ETL
        """Tại tầng DMT chúng ta sẽ thực hiện ETL ra các bảng Fact và các bảng Dim"""
        """Việc ETL tại tầng này sẽ tùy thuộc vào nghiệp vụ mà etl ra các table phục vụ phù hợp"""
        """Phần code phía dưới chỉ xử etl minh họa, được sử lý theo SCD Type 1 và có dùng Iceberg"""


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
        description="job _0401_customer_dim"
    )
    parser.add_argument("--etl_date", type=int, help="etl_date (YYYYMMDD)")
    args = parser.parse_args()

    success = _0401_customer_dim(etl_date=args.etl_date)
    print(f"Job completed - Success: {success}")
    exit(0 if success else 1)