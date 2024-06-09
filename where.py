from pyspark.sql import SparkSession
import time

# Khởi tạo SparkSession
spark = SparkSession.builder \
    .appName("SQLite with SparkSQL") \
    .config("spark.driver.extraClassPath", "/opt/spark/jars/sqlite-jdbc-3.34.0.jar") \
    .getOrCreate()

db_path = "jdbc:sqlite:/app/data_cua_thang.db"

# Tải dữ liệu từ cơ sở dữ liệu SQLite
df = spark.read.format("jdbc") \
    .option("url", db_path) \
    .option("dbtable", "mydata") \
    .option("driver", "org.sqlite.JDBC") \
    .load()

df.createOrReplaceTempView("mydata")

# Hàm thực hiện và tính thời gian truy vấn
def execute_query(query):
    start_time = time.time()
    result = spark.sql(query).collect()
    end_time = time.time()
    print(f"Query: {query}")
    print(f"Execution Time: {end_time - start_time} seconds")
    print(f"Result Count: {len(result)}\n")

# Truy vấn 1: Truy vấn cơ bản không có WHERE
query1 = "SELECT * FROM mydata"
execute_query(query1)

# Truy vấn 2: Truy vấn với mệnh đề WHERE đơn giản
query2 = "SELECT * FROM mydata WHERE ho = 'Nguyen_9'"
execute_query(query2)

# Truy vấn 3: Truy vấn với mệnh đề WHERE phức tạp hơn
query3 = "SELECT * FROM mydata WHERE ho = 'Nguyen_9' AND ten = 'Thang_9'"
execute_query(query3)

# Truy vấn 4: Sử dụng WHERE trên cột ten
query4 = "SELECT * FROM mydata WHERE ten = 'Thang_3'"
execute_query(query4)

# Truy vấn 5: Sử dụng WHERE trên các cột ho và ten_dem
query5 = "SELECT * FROM mydata WHERE ho = 'Nguyen_5' AND ten_dem = 'Huu_7'"
execute_query(query5)

# Truy vấn 6: Sử dụng WHERE trên cột ho hoặc ten
query6 = "SELECT * FROM mydata WHERE ho = 'Nguyen_3' OR ten = 'Thang_2'"
execute_query(query6)

# Truy vấn 7: Sử dụng WHERE phức tạp với AND và OR
query7 = "SELECT * FROM mydata WHERE (ho = 'Nguyen_4' AND ten_dem = 'Huu_9') OR ten = 'Thang_6'"
execute_query(query7)

# Truy vấn 8: Sử dụng WHERE với điều kiện LIKE
query8 = "SELECT * FROM mydata WHERE ten LIKE 'Thang_%'"
execute_query(query8)

# Truy vấn 9: Sử dụng WHERE với điều kiện BETWEEN
query9 = "SELECT * FROM mydata WHERE id BETWEEN 5 AND 15"
execute_query(query9)

# Truy vấn 10: Sử dụng WHERE với điều kiện IN
query10 = "SELECT * FROM mydata WHERE ten IN ('Thang_0', 'Thang_5', 'Thang_10')"
execute_query(query10)

# Dừng phiên Spark
spark.stop()
