from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("SQLite with SparkSQL") \
    .config("spark.driver.extraClassPath", "/opt/spark/jars/sqlite-jdbc-3.34.0.jar") \
    .getOrCreate()

db_path = "jdbc:sqlite:/app/data_cua_thang.db"

df = spark.read.format("jdbc") \
    .option("url", db_path) \
    .option("dbtable", "mydata") \
    .option("driver", "org.sqlite.JDBC") \
    .load()

df.createOrReplaceTempView("mydata")

print("Truy vấn 1: Lấy tối đa 20 dòng đầu tiên")
spark.sql("SELECT * FROM mydata LIMIT 20").show()

print("Truy vấn 2: Đếm số lượng dòng trong bảng")
spark.sql("SELECT COUNT(*) FROM mydata").show()

print("Truy vấn 3: Lấy các dòng có tên là 'Thang_1'")
spark.sql("SELECT * FROM mydata WHERE ten = 'Thang_1'").show()

print("Truy vấn 4: Đếm số lượng họ 'Nguyen_1'")
spark.sql("SELECT COUNT(*) FROM mydata WHERE ho = 'Nguyen_1'").show()

print("Truy vấn 5: Lấy 10 họ đầu tiên và đếm số lượng tương ứng")
spark.sql("SELECT ho, COUNT(*) as count FROM mydata GROUP BY ho ORDER BY ho LIMIT 10").show()


