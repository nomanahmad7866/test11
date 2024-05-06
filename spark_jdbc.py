from pyspark.sql import SparkSession
spark = SparkSession.builder \
    .appName("Making PostgreSQL Connection ") \
    .config("spark.jars", "file:///C:/Users/nomis/Downloads/postgresql-42.6.2.jar") \
    .getOrCreate()

jdbc_url = "jdbc:postgresql://localhost:5432/covid19_db"

connection_properties = {
    "user": "postgres",
    "password": "oyehoye1234",
    "driver": "org.postgresql.Driver"
}


import pdb;pdb.set_trace()
table1 = "confirmed"
table2 = 'deaths'
df1 = spark.read.jdbc(url=jdbc_url, table=table2, properties=connection_properties)
df1.show()
df2 = spark.read.jdbc(url=jdbc_url, table=table2, properties = connection_properties)

spark.stop()

spark

spark.sql('''
        select * from df1 where not exists 
        (select * from df2 where df1.country = df2.country)
        ''')