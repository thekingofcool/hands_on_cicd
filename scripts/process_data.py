# process_data.py

from pyspark.sql import SparkSession

# 创建 Spark 会话
spark = SparkSession.builder \
    .appName("Data Processing") \
    .getOrCreate()

# 读取 CSV 文件
input_file = "input_data.csv"
df = spark.read.csv(input_file, header=True, inferSchema=True)

# 数据处理：计算每个类别的平均值
result_df = df.groupBy("category").agg({"value": "avg"})

# 将结果写入新的 CSV 文件
output_file = "output_data.csv"
result_df.write.csv(output_file, header=True)

# 停止 Spark 会话
spark.stop()
