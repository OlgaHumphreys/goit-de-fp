
# Create project files for the user's final project (streaming + batch + Airflow).

import os, textwrap, json, datetime, pandas as pd

base = "/mnt/data/final_project"
streaming_dir = os.path.join(base, "streaming")
batch_dir = os.path.join(base, "batch")
airflow_dir = os.path.join(base, "airflow")
sql_dir = os.path.join(base, "sql")
os.makedirs(streaming_dir, exist_ok=True)
os.makedirs(batch_dir, exist_ok=True)
os.makedirs(airflow_dir, exist_ok=True)
os.makedirs(sql_dir, exist_ok=True)

# --------------- streaming/mysql_to_kafka.py ---------------
mysql_to_kafka = textwrap.dedent(r'''
    # -*- coding: utf-8 -*-
    """
    
    Starting:
      python mysql_to_kafka.py \
        --mysql-host localhost --mysql-port 3306 \
        --mysql-db olympic_dataset --mysql-user user --mysql-pass pass \
        --table athlete_event_results \
        --bootstrap localhost:9092 --topic athlete_event_results
    """
    import argparse, json, time
    from typing import Any, Dict, Iterable
    from kafka import KafkaProducer
    import mysql.connector

    def produce_rows(rows: Iterable[Dict[str, Any]], producer: KafkaProducer, topic: str):
        for row in rows:
            # Decimal/bytes → str/float 
            serializable = {}
            for k, v in row.items():
                if v is None:
                    serializable[k] = None
                elif isinstance(v, (bytes, bytearray)):
                    serializable[k] = v.decode('utf-8', errors='ignore')
                else:
                    try:
                        json.dumps(v)  # перевіряємо, що серіалізується
                        serializable[k] = v
                    except Exception:
                        serializable[k] = str(v)
            producer.send(topic, value=serializable)
        producer.flush()

    if __name__ == "__main__":
        ap = argparse.ArgumentParser()
        ap.add_argument("--mysql-host", required=True)
        ap.add_argument("--mysql-port", type=int, default=3306)
        ap.add_argument("--mysql-db", required=True)
        ap.add_argument("--mysql-user", required=True)
        ap.add_argument("--mysql-pass", required=True)
        ap.add_argument("--table", default="athlete_event_results")
        ap.add_argument("--bootstrap", required=True)
        ap.add_argument("--topic", default="athlete_event_results")
        ap.add_argument("--batch-size", type=int, default=5000, help="Максимум рядків за один пул")
        args = ap.parse_args()

        conn = mysql.connector.connect(
            host=args.mysql_host, port=args.mysql_port,
            database=args.mysql_db, user=args.mysql_user, password=args.mysql_pass
        )
        cursor = conn.cursor(dictionary=True)
        cursor.execute(f"SELECT * FROM {args.table}")
        rows = cursor.fetchmany(args.batch_size)

        producer = KafkaProducer(
            bootstrap_servers=args.bootstrap,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            linger_ms=50, acks="1"
        )

        total = 0
        while rows:
            produce_rows(rows, producer, args.topic)
            total += len(rows)
            rows = cursor.fetchmany(args.batch_size)

        print(f"Sent {total} rows from MySQL to Kafka topic '{args.topic}'.")
        cursor.close()
        conn.close()
''')

# --------------- streaming/stream_aggregate.py ---------------
stream_aggregate = textwrap.dedent(r'''
    # -*- coding: utf-8 -*-
    """
    Part 1 (Streaming): End-to-End pipeline
    Stages:

    1)  MySQL (olympic_dataset.athlete_bio).
    2)  height/weight.
    3)  athlete_event_results (JSON → columns).
        mysql_to_kafka.py 
    4) (results) athlete_id.
    5)  height/weight для комбінацій sport, medal, sex, country_noc + додати timestamp.
    6) forEachBatch MySQL, athlete_enriched_agg.

    Starting :
      spark-submit \
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,mysql:mysql-connector-j:8.3.0 \
        stream_aggregate.py \
        --bootstrap localhost:9092 \
        --out-topic athlete_enriched_agg \
        --mysql-url jdbc:mysql://localhost:3306/olympic_dataset \
        --mysql-user user --mysql-pass pass
    """
    import argparse
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import (
        from_json, col, avg, current_timestamp, to_json, struct, lit, when, regexp_replace, trim, broadcast
    )
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

    if __name__ == "__main__":
        ap = argparse.ArgumentParser()
        ap.add_argument("--bootstrap", required=True, help="Kafka bootstrap servers, напр. localhost:9092")
        ap.add_argument("--in-topic", default="athlete_event_results")
        ap.add_argument("--out-topic", default="athlete_enriched_agg")
        ap.add_argument("--mysql-url", required=True, help="jdbc:mysql://host:port/olympic_dataset")
        ap.add_argument("--mysql-user", required=True)
        ap.add_argument("--mysql-pass", required=True)
        ap.add_argument("--mysql-bio-table", default="olympic_dataset.athlete_bio")
        ap.add_argument("--mysql-out-table", default="oleksiy.athlete_enriched_agg", help="куди писати агрегати")
        args = ap.parse_args()

        spark = (
            SparkSession.builder
            .appName("FinalProject-Streaming-Aggregates")
            .config("spark.sql.shuffle.partitions", "8")
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel("WARN")

        # ---- Етап 1: біо-дані з MySQL ----
        bio = (
            spark.read.format("jdbc")
            .option("url", args.mysql_url)
            .option("dbtable", args.mysql_bio_table)
            .option("user", args.mysql_user)
            .option("password", args.mysql_pass)
            .load()
        )

        # ---- Stage 2:  height/weight ----
        #  trim; height/weight Double
        bio_clean = (
            bio.withColumn("height", col("height").cast("double"))
               .withColumn("weight", col("weight").cast("double"))
               .filter(col("height").isNotNull() & col("weight").isNotNull())
               .select("athlete_id", "sex", "country_noc", "height", "weight")
        )

        # ---- Stage 3: read from Kafka and JSON → columns ----
        kschema = StructType([
            StructField("athlete_id", IntegerType(), True),
            StructField("sport",      StringType(),  True),
            StructField("medal",      StringType(),  True),
        ])

        events = (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", args.bootstrap)
            .option("subscribe", args.in_topic)
            .option("startingOffsets", "latest")
            .load()
            .select(from_json(col("value").cast("string"), kschema).alias("j"))
            .select("j.athlete_id", "j.sport", "j.medal")
        )

        # 'NA'/'None'/'null' у medal до Null 
        events_norm = (
            events.withColumn("medal", when(col("medal").isin("NA", "NaN", "None", "null", ""), None).otherwise(col("medal")))
                  .withColumn("sport", trim(regexp_replace(col("sport"), r"\s+", " ")))
        )

        # ---- Stage 4: join за athlete_id ----
        joined = events_norm.join(broadcast(bio_clean), on="athlete_id", how="inner")

        # ---- Stage 5: aggregation ----
        agg = (
            joined.groupBy("sport", "medal", "sex", "country_noc")
                  .agg(
                      avg("height").alias("avg_height"),
                      avg("weight").alias("avg_weight")
                  )
                  .withColumn("timestamp", current_timestamp())
        )

        # ---- Stage 6: forEachBatch → Kafka та MySQL ----
        def sink_to_targets(df, epoch_id: int):
            # 6.a) out-put Kafka-top
            out_json = (
                df.select(
                    to_json(
                        struct("sport","medal","sex","country_noc","avg_height","avg_weight","timestamp")
                    ).alias("value")
                )
                .selectExpr("CAST(NULL AS STRING) AS key", "CAST(value AS STRING)")
            )
            (out_json.write
                .format("kafka")
                .option("kafka.bootstrap.servers", args.bootstrap)
                .option("topic", args.out_topic)
                .save())

            # 6.b) MySQL 
            (df.write
                .mode("append")
                .format("jdbc")
                .option("url", args.mysql_url)
                .option("dbtable", args.mysql_out_table)
                .option("user", args.mysql_user)
                .option("password", args.mysql_pass)
                .option("driver", "com.mysql.cj.jdbc.Driver")
                .save())

        query = (
            agg.writeStream
               .outputMode("update")   # update/complete
               .foreachBatch(sink_to_targets)
               .option("checkpointLocation", "./chk_streaming_agg")
               .start()
        )

        query.awaitTermination()
''')

# --------------- batch/landing_to_bronze.py ---------------
landing_to_bronze = textwrap.dedent(r'''
    # -*- coding: utf-8 -*-
    """
    Part 2: Data Lake (Batch)
    Етап 1: landing -> bronze
      - Load CSV з (s)ftp/http
      - Read CSV in Spark
      - Record as Parquet up to bronze/{table}

    Examples:
      spark-submit landing_to_bronze.py \
        --url https://ftp.goit.study/neoversity/athlete_bio.csv \
        --table athlete_bio --outdir ./bronze
    """
    import argparse, os, urllib.request
    from pyspark.sql import SparkSession

    if __name__ == "__main__":
        ap = argparse.ArgumentParser()
        ap.add_argument("--url", required=True)
        ap.add_argument("--table", required=True)
        ap.add_argument("--outdir", default="./bronze")
        args = ap.parse_args()

        os.makedirs(args.outdir, exist_ok=True)
        local_csv = os.path.join(args.outdir, f"{args.table}.csv")

        # Loading у landing (csv)
        urllib.request.urlretrieve(args.url, local_csv)

        spark = SparkSession.builder.appName("landing_to_bronze").getOrCreate()
        spark.sparkContext.setLogLevel("WARN")

        df = (spark.read
                    .option("header", True)
                    .option("inferSchema", True)
                    .csv(local_csv))

        bronze_path = os.path.join(args.outdir, args.table)
        (df.write.mode("overwrite").parquet(bronze_path))
        print(f"Wrote bronze table to: {bronze_path}")
''')

# --------------- batch/bronze_to_silver.py ---------------
bronze_to_silver = textwrap.dedent(r'''
    # -*- coding: utf-8 -*-
    """
    Part 2: Data Lake (Batch)
    Stage 2: bronze -> silver
      - Read bronze/{table} (parquet)
      - Clean text columns (trim)
      - Deduplication
      - Record silver/{table}
    """
    import argparse, os
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import regexp_replace, trim, col
    from pyspark.sql.types import StringType

    if __name__ == "__main__":
        ap = argparse.ArgumentParser()
        ap.add_argument("--table", required=True)
        ap.add_argument("--bronze", default="./bronze")
        ap.add_argument("--silver", default="./silver")
        args = ap.parse_args()

        spark = SparkSession.builder.appName("bronze_to_silver").getOrCreate()
        spark.sparkContext.setLogLevel("WARN")

        src = os.path.join(args.bronze, args.table)
        df = spark.read.parquet(src)

        # Function for clean text columns
        cleaned = df
        for f in df.schema.fields:
            if isinstance(f.dataType, StringType):
                c = col(f.name)
                c = trim(regexp_replace(c, r"\s+", " "))           # згортаємо мультипропуски
                c = regexp_replace(c, r"[\r\n\t]", "")              # прибираємо керуючі
                cleaned = cleaned.withColumn(f.name, c)

        # Deduplication
        cleaned = cleaned.dropDuplicates()

        out = os.path.join(args.silver, args.table)
        (cleaned.write.mode("overwrite").parquet(out))
        print(f"Wrote silver table to: {out}")
''')

# --------------- batch/silver_to_gold.py ---------------
silver_to_gold = textwrap.dedent(r'''
    # -*- coding: utf-8 -*-
    """
    Part 2: Data Lake (Batch)
    Stage 3: silver -> gold/avg_stats
      - Read silver/athlete_bio та silver/athlete_event_results
      - Join за athlete_id
      - Group sport, medal, sex, country_noc
      - Average weight/height + timestamp
      - Record gold/avg_stats
    """
    import argparse, os
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import avg, current_timestamp, col

    if __name__ == "__main__":
        ap = argparse.ArgumentParser()
        ap.add_argument("--silver", default="./silver")
        ap.add_argument("--gold", default="./gold")
        ap.add_argument("--bio", default="athlete_bio")
        ap.add_argument("--events", default="athlete_event_results")
        args = ap.parse_args()

        spark = SparkSession.builder.appName("silver_to_gold").getOrCreate()
        spark.sparkContext.setLogLevel("WARN")

        bio = spark.read.parquet(os.path.join(args.silver, args.bio))
        events = spark.read.parquet(os.path.join(args.silver, args.events))

        # Min clean
        bio = bio.withColumn("height", col("height").cast("double")) \
                 .withColumn("weight", col("weight").cast("double")) \
                 .filter(col("height").isNotNull() & col("weight").isNotNull())

        joined = events.join(bio.select("athlete_id","sex","country_noc","height","weight"), on="athlete_id", how="inner")

        gold_df = (joined.groupBy("sport","medal","sex","country_noc")
                        .agg(avg("height").alias("avg_height"), avg("weight").alias("avg_weight"))
                        .withColumn("timestamp", current_timestamp()))

        out = os.path.join(args.gold, "avg_stats")
        (gold_df.write.mode("overwrite").parquet(out))
        print(f"Wrote gold table to: {out}")
''')

# --------------- airflow/project_solution.py ---------------
project_solution = textwrap.dedent(r'''
    # -*- coding: utf-8 -*-
    """
    Airflow DAG: 
    """
    from datetime import datetime
    from airflow import DAG
    from airflow.operators.bash import BashOperator

    # Path
    BASE = "/opt/airflow/dags/final_project/batch"  # example
    BRONZE = "/opt/airflow/dags/final_project/bronze"
    SILVER = "/opt/airflow/dags/final_project/silver"
    GOLD = "/opt/airflow/dags/final_project/gold"

    default_args = {"owner": "student", "retries": 0}

    with DAG(
        dag_id="project_solution_batch_datalake",
        start_date=datetime(2024,1,1),
        schedule=None,
        catchup=False,
        default_args=default_args,
        tags=["final","batch","spark"]
    ) as dag:

        # 1) landing -> bronze (athlete_bio)
        landing_bio = BashOperator(
            task_id="landing_to_bronze_bio",
            bash_command=(
                "spark-submit {{ var.value.SPARK_PACKAGES }} "
                f"{BASE}/landing_to_bronze.py --url https://ftp.goit.study/neoversity/athlete_bio.csv "
                f"--table athlete_bio --outdir {BRONZE}"
            )
        )

        # 1) landing -> bronze (athlete_event_results)
        landing_events = BashOperator(
            task_id="landing_to_bronze_events",
            bash_command=(
                "spark-submit {{ var.value.SPARK_PACKAGES }} "
                f"{BASE}/landing_to_bronze.py --url https://ftp.goit.study/neoversity/athlete_event_results.csv "
                f"--table athlete_event_results --outdir {BRONZE}"
            )
        )

        # 2) bronze -> silver (обидві таблиці)
        bronze2silver_bio = BashOperator(
            task_id="bronze_to_silver_bio",
            bash_command=(
                "spark-submit "
                f"{BASE}/bronze_to_silver.py --table athlete_bio --bronze {BRONZE} --silver {SILVER}"
            )
        )

        bronze2silver_events = BashOperator(
            task_id="bronze_to_silver_events",
            bash_command=(
                "spark-submit "
                f"{BASE}/bronze_to_silver.py --table athlete_event_results --bronze {BRONZE} --silver {SILVER}"
            )
        )

        # 3) silver -> gold
        silver2gold = BashOperator(
            task_id="silver_to_gold",
            bash_command=(
                "spark-submit "
                f"{BASE}/silver_to_gold.py --silver {SILVER} --gold {GOLD} "
                "--bio athlete_bio --events athlete_event_results"
            )
        )

        [landing_bio, landing_events] >> [bronze2silver_bio, bronze2silver_events] >> silver2gold
''')

# --------------- sql/create_agg_table.sql ---------------
create_sql = textwrap.dedent(r'''
    -- Create table (Streaming) 
    CREATE TABLE IF NOT EXISTS oleksiy.athlete_enriched_agg (
        sport        VARCHAR(100),
        medal        VARCHAR(16),
        sex          VARCHAR(8),
        country_noc  VARCHAR(8),
        avg_height   DECIMAL(10,3),
        avg_weight   DECIMAL(10,3),
        timestamp    DATETIME
    );
''')

# --------------- README ---------------
readme = textwrap.dedent(r'''
    # Final Project: Streaming + Data Lake + Airflow (starter pack)

    ## Structure
    - `streaming/mysql_to_kafka.py` — read MySQL `olympic_dataset.athlete_event_results` Kafka `athlete_event_results`.
    - `streaming/stream_aggregate.py` — Spark Structured Streaming: read `athlete_bio` з MySQL, Kafka, join, aggregation → (Kafka + MySQL).
    - `sql/create_agg_table.sql` — Create table `athlete_enriched_agg`.
    - `batch/landing_to_bronze.py` — Load CSV → bronze (parquet).
    - `batch/bronze_to_silver.py` — Dedublication → silver.
    - `batch/silver_to_gold.py` — join + agregate → gold/avg_stats.
    - `airflow/project_solution.py` — DAG (BashOperator + spark-submit).

    ## Spark
    MySQL JDBC to add:
    `--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,mysql:mysql-connector-j:8.3.0`

    ## Example load streaming
    1) `sql/create_agg_table.sql`.
    2) Load в Kafka події з MySQL:
       ```bash
       pip install kafka-python mysql-connector-python
       python streaming/mysql_to_kafka.py --mysql-host ... --mysql-db olympic_dataset \
         --mysql-user ... --mysql-pass ... --bootstrap localhost:9092 --topic athlete_event_results
       ```
    3) Load Spark job:
       ```bash
       spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,mysql:mysql-connector-j:8.3.0 \
         streaming/stream_aggregate.py \
         --bootstrap localhost:9092 \
         --mysql-url jdbc:mysql://localhost:3306/olympic_dataset \
         --mysql-user ... --mysql-pass ... \
         --mysql-out-table oleksiy.athlete_enriched_agg
       ```
    4) Kafka-topic `athlete_enriched_agg` MySQL.

    ## Airflow
    - Move files from `batch/` and DAG Airflow.
    - `Variables` `SPARK_PACKAGES` with unnecessary `--packages`.
''')

# Write files
with open(os.path.join(streaming_dir, "mysql_to_kafka.py"), "w", encoding="utf-8") as f:
    f.write(mysql_to_kafka)

with open(os.path.join(streaming_dir, "stream_aggregate.py"), "w", encoding="utf-8") as f:
    f.write(stream_aggregate)

with open(os.path.join(batch_dir, "landing_to_bronze.py"), "w", encoding="utf-8") as f:
    f.write(landing_to_bronze)

with open(os.path.join(batch_dir, "bronze_to_silver.py"), "w", encoding="utf-8") as f:
    f.write(bronze_to_silver)

with open(os.path.join(batch_dir, "silver_to_gold.py"), "w", encoding="utf-8") as f:
    f.write(silver_to_gold)

with open(os.path.join(airflow_dir, "project_solution.py"), "w", encoding="utf-8") as f:
    f.write(project_solution)

with open(os.path.join(sql_dir, "create_agg_table.sql"), "w", encoding="utf-8") as f:
    f.write(create_sql)

with open(os.path.join(base, "README.md"), "w", encoding="utf-8") as f:
    f.write(readme)

print("Created project files at:", base)
