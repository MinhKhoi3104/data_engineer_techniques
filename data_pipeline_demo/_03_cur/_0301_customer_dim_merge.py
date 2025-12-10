import os, sys
from pyspark.sql.functions import *
from datetime import date
current_dir = os.path.dirname(__file__)
config_path = os.path.join(current_dir, '..','..')
config_path = os.path.abspath(config_path)
sys.path.insert(0, config_path)
from _001_config._00101_database_config import JDBC_URL_POSTGRES
from _002_utils._00201_utils import *

def _0301_customer_dim_merge(etl_date=None):
    try:
        # Tại tầng này sẽ ứng dụng Iceberg
        spark = create_spark_session_iceberg("_0301_customer_dim_merge")
        spark_non_iceberg = create_spark_session("_0301_customer_dim_merge")

        if etl_date is None:
            etl_date = int(date.today().strftime("%Y%m%d"))

        # Tạo namespace trong iceberg
        spark.sql("CREATE NAMESPACE IF NOT EXISTS cur")
        
        # Tạo bảng target nếu chưa tồn tại trong iceberg
        spark.sql("""drop table if exists jdbciceberg.cur.customer""")
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
        sql_create_schema = """CREATE SCHEMA IF NOT EXISTS cur"""
        execute_sql_ddl (
                spark = spark,
                sql_query = sql_create_schema,
                jdbc_url = JDBC_URL_POSTGRES["url"],
                properties = JDBC_URL_POSTGRES["properties"]
            )
        
        # Tạo bảng target nếu chưa tồn tại trong postgreSQL
        sql_create_target_tbl= """
            CREATE TABLE IF NOT EXISTS cur.customer(
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
        
        # Lấy dữ liệu từ prc để xử lý tại cur
        source_df = spark_non_iceberg.read\
            .jdbc(url=JDBC_URL_POSTGRES["url"],\
            table= "(SELECT * FROM prc.customer) a",
            properties=JDBC_URL_POSTGRES["properties"])
        
        # Lấy dữ liệu target từ prc
        target_df = spark.read.format("iceberg").load("jdbciceberg.cur.customer")

        
        # Thực hiện ETL dữ liệu
        """Phần code phía dưới chỉ xử etl minh họa, được sử lý theo SCD Type 1 và có dùng Iceberg"""
        
        cols = [col for col in target_df.columns if col not in ("etl_date","is_active")]
        ### Lọc ra dữ liệu insert và update
        insert_update_df = \
            source_df.select(cols).subtract(target_df.select(cols))
        
        insert_update_df = \
            insert_update_df\
                .withColumn("is_active",lit("1"))\
                .withColumn("etl_date", to_date(lit(etl_date), "yyyyMMdd"))
        
        ### Lọc dữ liệu bị delete
        delete_df = \
            target_df.select(cols).alias("t")\
            .join(source_df.alias("s"), col("t.customer_id") == col("s.customer_id"), "left_anti")\
            .select("t.*")
        
        delete_df = \
            delete_df\
                .withColumn("is_active",lit("0"))\
                .withColumn("etl_date", to_date(lit(etl_date), "yyyyMMdd"))

        
        # Dữ liệu thay đổi là
        customer_diff_df = insert_update_df.union(delete_df)

        if customer_diff_df.count() > 0:
            print(f"Số dữ liệu cần inser_update: {customer_diff_df.count()}")
            customer_diff_df.show(10, truncate=False)
            # Tạo view tạm cho bảng customer_diff_df
            customer_diff_df.createOrReplaceTempView("customer_diff_df")
            spark.sql("""
            MERGE INTO jdbciceberg.cur.customer AS tgt
            USING customer_diff_df AS src
                on tgt.customer_id = src.customer_id
            WHEN MATCHED THEN 
                UPDATE SET
                    tgt.customer_name = src.customer_name,
                    tgt.customer_birth = src.customer_birth,
                    tgt.customer_hometown = src.customer_hometown,
                    tgt.customer_phone_num = src.customer_phone_num,
                    tgt.customer_email = src.customer_email,
                    tgt.customer_source = src.customer_source,
                    tgt.customer_gender = src.customer_gender,
                    tgt.is_active = src.is_active,
                    tgt.etl_date = src.etl_date
            WHEN NOT MATCHED THEN
                INSERT (tgt.customer_id, tgt.customer_name, tgt.customer_birth, tgt.customer_hometown, tgt.customer_phone_num, 
                    tgt.customer_email, tgt.customer_source, tgt.customer_gender, tgt.is_active, tgt.etl_date) 
                VALUES (src.customer_id, src.customer_name, src.customer_birth, src.customer_hometown, src.customer_phone_num, 
                    src.customer_email, src.customer_source, src.customer_gender, src.is_active,src.etl_date)
            """)
        
            print("✅ Đã ghi dữ liệu thành công vào jdbciceberg.cur.customer")

            new_target_df = spark.read.format("iceberg").load("jdbciceberg.cur.customer")
            # Đưa dữ liệu về postgresql
            new_target_df.write \
                .format("jdbc") \
                .mode("overwrite") \
                .option("url", JDBC_URL_POSTGRES["url"]) \
                .option("dbtable", "cur.customer") \
                .options(**JDBC_URL_POSTGRES["properties"]) \
                .save()
            print(f"✅ Ghi dữ liệu thành công vào bảng cur.customer")

        else: 
            print("Không có bản ghi nào cần thực hiện Insert hoặc Update")
        

        return True
    except Exception as e:
        print(f"ERROR: {e}")
        return False

    finally:
        if spark is not None:
            spark.stop()
        if spark_non_iceberg is not None:
            spark_non_iceberg.stop()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="job _0301_customer_dim_merge"
    )
    parser.add_argument("--etl_date", type=int, help="etl_date (YYYYMMDD)")
    args = parser.parse_args()

    success = _0301_customer_dim_merge(etl_date=args.etl_date)
    print(f"Job completed - Success: {success}")
    exit(0 if success else 1)
