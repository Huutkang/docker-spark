from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, when
import os


spark = SparkSession.builder \
    .appName("Spark crud") \
    .getOrCreate()

db_path = "jdbc:sqlite:/app/data_cua_thang.db"

df = spark.read.format("jdbc") \
    .option("url", db_path) \
    .option("dbtable", "mydata") \
    .option("driver", "org.sqlite.JDBC") \
    .load()

df.show(10)


new_data = [(11, "Nguyen_11", "Huu_11", "Thang_11"),
            (12, "Nguyen_12", "Huu_12", "Thang_12")]

new_df = spark.createDataFrame(new_data, ["id", "ho", "ten_dem", "ten"])
new_df.show()

df = df.union(new_df)
df.createOrReplaceTempView("mydata")

print("Truy vấn 2: Đếm số lượng dòng trong bảng")
spark.sql("SELECT COUNT(*) FROM mydata").show()


print("READ: Hiển thị dòng với id = 11")
spark.sql("SELECT * FROM mydata WHERE id = 11").show()

print("UPDATE: Cập nhật dữ liệu")
df = df.withColumn("ten", when(df.id == 1, "Updated_Thang").otherwise(df.ten))
df.createOrReplaceTempView("mydata")
spark.sql("SELECT * FROM mydata WHERE id = 1").show()

print("DELETE: Xóa các dòng với id = 3")
df = df.where(df.id != 3)
df.createOrReplaceTempView("mydata")
spark.sql("SELECT * FROM mydata WHERE id != 3").show()

df.show(10)

print("Truy vấn 2: Đếm số lượng dòng trong bảng")
spark.sql("SELECT COUNT(*) FROM mydata").show()



df.write.format("jdbc") \
    .option("url", "jdbc:sqlite:/app/x.db?busy_timeout=5000") \
    .option("dbtable", "mydata") \
    .option("driver", "org.sqlite.JDBC") \
    .mode("overwrite") \
    .save()

spark.stop()


os.system('rm /app/data_cua_thang.db')
os.system('mv /app/x.db /app/data_cua_thang.db')


