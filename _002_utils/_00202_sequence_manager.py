import datetime
import threading
import time
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import expr, lit, monotonically_increasing_id
from typing import Dict, Tuple, Optional


class SequenceManager:
    """
    Quản lý sequence Iceberg (auto-increment ID) cho Spark job.
    Tối ưu cho hiệu suất và xử lý song song.
    """
    
    # Singleton instance để dùng chung
    _instance = None
    _lock = threading.RLock()  # Use RLock to prevent deadlocks
    _lock_timeout = 10  # Timeout in seconds
    
    # Cache cho sequence values để tránh đọc lại từ DB
    _sequence_cache: Dict[str, int] = {}
    
    @classmethod
    def get_instance(cls, spark=None, catalog_name="jdbciceberg", table_name="meta.sequence_state"):
        """Singleton pattern để đảm bảo chỉ có một instance được sử dụng"""
        if cls._instance is None:
            # Use timeout to avoid deadlock
            lock_acquired = cls._lock.acquire(timeout=cls._lock_timeout)
            if not lock_acquired:
                print("Warning: Lock timeout in get_instance, creating new instance anyway")
            try:
                if cls._instance is None:
                    cls._instance = cls(spark, catalog_name, table_name)
            finally:
                if lock_acquired:
                    cls._lock.release()
        return cls._instance
    
    def __init__(self, spark=None, catalog_name="jdbciceberg", table_name="meta.sequence_state", default_block_size=1000):
        """
        Khởi tạo SequenceManager với các tham số mặc định.
        """
        if spark is None:
            from pyspark.sql import SparkSession
            spark = SparkSession.builder.getOrCreate()
            
        self.spark = spark
        self.catalog_name = catalog_name
        self.table_name = table_name
        self.full_table = f"{catalog_name}.{table_name}"
        self.default_block_size = default_block_size
        
        # Ensure sequence table exists (with retries)
        max_retries = 3
        for attempt in range(1, max_retries + 1):
            try:
                self._ensure_sequence_table_exists()
                break
            except Exception as e:
                if attempt == max_retries:
                    print(f"Failed to ensure sequence table after {max_retries} attempts")
                time.sleep(1)
    
    def _ensure_sequence_table_exists(self):
        """Đảm bảo bảng sequence đã tồn tại"""
        try:
            # Attempt to read from table to check if it exists
            self.spark.sql(f"SELECT 1 FROM {self.full_table} LIMIT 1")
        except Exception:
            # Parse table components
            parts = self.full_table.split(".")
            if len(parts) == 3:  # catalog.namespace.table format
                catalog, namespace, table_name = parts
            else:  # catalog.table format
                catalog = parts[0]
                namespace = "meta"
                table_name = parts[1]
            
            # Create namespace if needed
            self.spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog}.{namespace}")
            
            # Create table with a simplified schema
            self.spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {self.full_table} (
                    sequence_name STRING,
                    current_value BIGINT,
                    last_updated TIMESTAMP
                ) USING iceberg
            """)
    
    def get_current_value(self, seq_name: str) -> int:
        """
        Lấy giá trị hiện tại của sequence.
        """
        # Check cache first (thread-safe)
        if seq_name in self._sequence_cache:
            return self._sequence_cache[seq_name]
            
        # Read from DB with timeout
        lock_acquired = self._lock.acquire(timeout=self._lock_timeout)
        if not lock_acquired:
            return 0
            
        try:
            # Query with timeout protection
            try:
                # Use direct SQL for better error handling
                result = self.spark.sql(
                    f"SELECT current_value FROM {self.full_table} WHERE sequence_name = '{seq_name}'"
                ).collect()
                
                if len(result) == 0:
                    # Create new sequence
                    # Use CURRENT_TIMESTAMP() function instead of string formatting
                    self.spark.sql(
                        f"INSERT INTO {self.full_table} VALUES ('{seq_name}', 0, CURRENT_TIMESTAMP())"
                    )
                    self._sequence_cache[seq_name] = 0
                    return 0
                    
                current_value = result[0]['current_value']
                self._sequence_cache[seq_name] = current_value
                return current_value
                
            except Exception:
                # Fallback to default value
                return 0
                
        finally:
            self._lock.release()
    
    def allocate_block(self, seq_name: str, block_size: Optional[int] = None) -> Tuple[int, int]:
        """
        Cấp phát 1 block ID liên tục cho job hiện tại.
        """
        if block_size is None or block_size <= 0:
            block_size = max(1, self.default_block_size)
            
        # Get lock with timeout
        lock_acquired = self._lock.acquire(timeout=self._lock_timeout)
        if not lock_acquired:
            return (1, block_size)  # Fallback values
            
        try:
            # Simple direct approach with retries
            max_retries = 3
            for attempt in range(1, max_retries + 1):
                try:
                    # Get current value
                    current_val = self.get_current_value(seq_name)
                    
                    # Calculate new value
                    new_val = current_val + block_size
                    
                    # Use CURRENT_TIMESTAMP() function in SQL for proper timestamp handling
                    self.spark.sql(f"""
                        MERGE INTO {self.full_table} t
                        USING (SELECT '{seq_name}' as seq_name) s
                        ON t.sequence_name = s.seq_name
                        WHEN MATCHED THEN UPDATE SET 
                            current_value = {new_val}, 
                            last_updated = CURRENT_TIMESTAMP()
                        WHEN NOT MATCHED THEN INSERT
                            (sequence_name, current_value, last_updated)
                            VALUES ('{seq_name}', {new_val}, CURRENT_TIMESTAMP())
                    """)
                    
                    # Update cache
                    self._sequence_cache[seq_name] = new_val
                    
                    start_id = current_val + 1
                    return (start_id, new_val)
                    
                except Exception:
                    if attempt == max_retries:
                        # Return fallback values
                        return (1, block_size)
                    else:
                        time.sleep(1)
                        
        finally:
            self._lock.release()
    
    @staticmethod
    def auto_id(df: DataFrame, seq_name: str, key_column_name: str, spark=None, 
                estimate_count: Optional[int] = None) -> DataFrame:
        """
        Simplified auto_id implementation that's more robust.
        """
        try:
            # Ensure we have a SparkSession
            if spark is None:
                from pyspark.sql import SparkSession
                spark = SparkSession.builder.getOrCreate()
            
            # Get count if not provided
            if estimate_count is None:
                estimate_count = df.count()
            
            # Simple direct approach without complex error handling
            try:
                # Create manager and allocate IDs
                manager = SequenceManager(spark)
                start_id, end_id = manager.allocate_block(seq_name, estimate_count)
                
                # Apply IDs to dataframe
                result_df = df.withColumn(
                    key_column_name,
                    expr(f"row_number() over (order by 1) + {start_id - 1}")
                )
                
                return result_df
                
            except Exception:
                return df.withColumn(key_column_name, expr("row_number() over (order by 1)"))
                
        except Exception:
            # Last resort fallback
            return df.withColumn(key_column_name, monotonically_increasing_id() + 1)

    # Simple utility methods with better error handling
    def assign_id_to_df(self, df: DataFrame, seq_name: str, key_column_name: str) -> DataFrame:
        """Simplified utility method for ID assignment with better error handling"""
        try:
            # Get count
            record_count = df.count()
            
            # Allocate block
            start_id, _ = self.allocate_block(seq_name, record_count)
            
            # Apply IDs
            return df.withColumn(
                key_column_name,
                expr(f"row_number() over (order by 1) + {start_id - 1}")
            )
        except Exception:
            return df.withColumn(key_column_name, expr("row_number() over (order by 1)"))
        
    def reset_sequence(self, seq_name: str, reset_value: int = 0) -> bool:
        """
        Reset sequence về giá trị chỉ định (mặc định = 0).
        """
        lock_acquired = self._lock.acquire(timeout=self._lock_timeout)
        if not lock_acquired:
            print(f"Could not acquire lock to reset sequence {seq_name}")
            return False

        try:
            # Cập nhật giá trị trong bảng Iceberg
            self.spark.sql(f"""
                MERGE INTO {self.full_table} t
                USING (SELECT '{seq_name}' AS seq_name) s
                ON t.sequence_name = s.seq_name
                WHEN MATCHED THEN UPDATE SET 
                    current_value = {reset_value}, 
                    last_updated = CURRENT_TIMESTAMP()
                WHEN NOT MATCHED THEN INSERT 
                    (sequence_name, current_value, last_updated)
                    VALUES ('{seq_name}', {reset_value}, CURRENT_TIMESTAMP())
            """)

            # Cập nhật cache
            self._sequence_cache[seq_name] = reset_value
            return True

        except Exception as e:
            print(f"Lỗi khi reset sequence '{seq_name}': {e}")
            return False
        finally:
            self._lock.release()
    