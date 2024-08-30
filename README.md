

![Untitled Diagram-2](https://github.com/user-attachments/assets/11adc90b-894f-4aa0-a8a8-4fc2350b3b95)


### Download the jar 

* hudi-extensions-0.1.0-SNAPSHOT-bundled.jar
Link https://drive.google.com/drive/folders/1ONmQzq-Fqj_mbhV4jZcS7yHVII8AK3LU?usp=share_link
  
* hudi-java-client-0.14.0.jar
  Link https://repo1.maven.org/maven2/org/apache/hudi/hudi-java-client/0.14.0/hudi-java-client-0.14.0.jar
  
* hudi-utilities-slim-bundle_2.12-0.14.0.jar
  Link : https://repo1.maven.org/maven2/org/apache/hudi/hudi-utilities_2.12/0.14.0/hudi-utilities_2.12-0.14.0.jar
  

# Create SnowFlake Tables 
```
CREATE DATABASE TEMPDB;

USE TEMPDB;

CREATE SCHEMA IF NOT EXISTS stage;

CREATE OR REPLACE TABLE stage.people (
    id INT,                     -- Unique identifier for each record
    name STRING,               -- Name of the person
    age INT,                   -- Age of the person
    city STRING,               -- City where the person resides
    create_ts TIMESTAMP        -- Timestamp of record creation
);

INSERT INTO stage.people (id, name, age, city, create_ts)
VALUES
    (1, 'John', 25, 'NYC', '2023-09-28 00:00:00'),
    (2, 'Emily', 30, 'SFO', '2023-09-29 00:00:00'),
    (3, 'Michael', 35, 'ORD', '2023-09-28 00:00:00');

```



set env var
```
export SNOWFLAKE_USER="XX"
export SNOWFLAKE_PWD="XXX"
export SNOWFLAKE_JDBC_URI="XXX"
```
# DeltaStreame job
```
spark-submit \
    --class org.apache.hudi.utilities.streamer.HoodieStreamer \
    --packages 'org.apache.hudi:hudi-spark3.4-bundle_2.12:0.14.0,net.snowflake:spark-snowflake_2.12:2.15.0-spark_3.4,net.snowflake:snowflake-jdbc:3.13.28' \
    --properties-file spark-config.properties \
    --master 'local[*]' \
    --executor-memory 1g \
    --jars '/Users/soumilshah/IdeaProjects/SparkProject/apache-hudi-delta-streamer-labs/E1/jar/hudi-extensions-0.1.0-SNAPSHOT-bundled.jar,/Users/soumilshah/IdeaProjects/SparkProject/apache-hudi-delta-streamer-labs/E1/jar/hudi-java-client-0.14.0.jar' \
    /Users/soumilshah/IdeaProjects/SparkProject/apache-hudi-delta-streamer-labs/E1/jar/hudi-utilities-slim-bundle_2.12-0.14.0.jar \
    --table-type COPY_ON_WRITE \
    --op UPSERT \
    --enable-sync \
    --sync-tool-classes 'io.onetable.hudi.sync.OneTableSyncTool' \
    --source-ordering-field create_ts \
    --source-class org.apache.hudi.utilities.sources.JdbcSource \
    --target-base-path 'file:///Users/soumilshah/IdeaProjects/SparkProject/apache-hudi-delta-streamer-labs/E1/silver/' \
    --target-table people \
    --transformer-class org.apache.hudi.utilities.transform.SqlQueryBasedTransformer \
    --hoodie-conf hoodie.datasource.write.keygenerator.class=org.apache.hudi.keygen.SimpleKeyGenerator \
    --hoodie-conf hoodie.datasource.write.recordkey.field=id \
    --hoodie-conf hoodie.datasource.write.partitionpath.field=city \
    --hoodie-conf hoodie.datasource.write.precombine.field=create_ts \
    --hoodie-conf hoodie.streamer.jdbc.url=$SNOWFLAKE_JDBC_URI \
    --hoodie-conf hoodie.streamer.jdbc.user=$SNOWFLAKE_USER \
    --hoodie-conf hoodie.streamer.jdbc.password=$SNOWFLAKE_PWD \
    --hoodie-conf hoodie.streamer.jdbc.driver.class=net.snowflake.client.jdbc.SnowflakeDriver \
    --hoodie-conf hoodie.streamer.jdbc.table.name=tempdb.stage.people \
    --hoodie-conf hoodie.streamer.jdbc.table.incr.column.name=id \
    --hoodie-conf hoodie.streamer.jdbc.incr.pull=true \
    --hoodie-conf hoodie.streamer.jdbc.incr.fallback.to.full.fetch=true \
    --hoodie-conf "hoodie.deltastreamer.transformer.sql=SELECT id, name, age, city, create_ts FROM <SRC> a;" \
    --hoodie-conf 'hoodie.onetable.formats.to.sync=DELTA,ICEBERG' \
    --hoodie-conf 'hoodie.onetable.target.metadata.retention.hr=168'
```

# Read as Icberg 
```
from pyspark.sql import SparkSession
import os
import sys

SPARK_VERSION = '3.4'
ICEBERG_VERSION = '0.12.0'  # Adjust the Iceberg version based on compatibility

SUBMIT_ARGS = f"--packages org.apache.hadoop:hadoop-aws:3.3.2,org.apache.iceberg:iceberg-spark3-runtime:{ICEBERG_VERSION} pyspark-shell"
os.environ["PYSPARK_SUBMIT_ARGS"] = SUBMIT_ARGS
os.environ['PYSPARK_PYTHON'] = sys.executable

# Use --packages and --conf options directly in PySpark code
spark = SparkSession.builder \
    .appName("IcebergReadExample") \
    .config("spark.sql.catalog.ic_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .getOrCreate()

# Read data from Iceberg table
iceberg_table_path = "file:///Users/soumilshah/IdeaProjects/SparkProject/apache-hudi-delta-streamer-labs/E1/silver"
df = spark.read.format("iceberg").load(iceberg_table_path)

# Show DataFrame
df.show()
```
![Screenshot 2024-08-30 at 5 45 48 PM](https://github.com/user-attachments/assets/12446ead-5d5d-4e3f-850a-a648144451d3)

# Read as Delta
```
from pyspark.sql import SparkSession
import os
import sys

SPARK_VERSION = '3.4'
DELTA_VERSION = '2.4.0'  # Adjust the Delta version based on compatibility

SUBMIT_ARGS = f"--packages org.apache.hadoop:hadoop-aws:3.3.2,io.delta:delta-core_2.12:{DELTA_VERSION} pyspark-shell"
os.environ["PYSPARK_SUBMIT_ARGS"] = SUBMIT_ARGS
os.environ['PYSPARK_PYTHON'] = sys.executable

# Use --packages and --conf options directly in PySpark code
spark = SparkSession.builder \
    .appName("DeltaReadExample") \
    .config('spark.sql.extensions', 'io.delta.sql.DeltaSparkSessionExtension') \
    .config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.delta.catalog.DeltaCatalog') \
    .getOrCreate()

# Read data from Delta table
delta_table_path = "file:///Users/soumilshah/IdeaProjects/SparkProject/apache-hudi-delta-streamer-labs/E1/silver/"

spark.read.format("delta").load(delta_table_path).createTempView("temp")

spark.sql("select *from temp").show()
```

![Screenshot 2024-08-30 at 5 45 48 PM](https://github.com/user-attachments/assets/dc95805a-0634-4fc1-90e1-f46bf204bdf6)

![mission-impossible-we-got-this](https://github.com/user-attachments/assets/0246dd37-e089-4c16-a797-64c6a9b88ea7)

