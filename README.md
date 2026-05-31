# STQD6324 Assignment 2 — MovieLens Data Pipeline

Data pipeline using Apache Spark, HDFS, and Cassandra on the MovieLens 100k dataset.  
Also did the optional HBase extension for Task ii and iii.

Dataset: [MovieLens 100k](https://grouplens.org/datasets/movielens/)  
Files used: `u.data`, `u.user`, `u.item`

---

## Tasks

- i. Average rating for each movie
- ii. Top 10 movies by average rating
- iii. Favourite genre for users with at least 50 ratings
- iv. Users under 20
- v. Scientist users aged 30–40

---

## Repository Structure

```text
MovieLens_Data_Management/
├── README.md
├── assignment2_notebook.ipynb
├── assignment2_movielens.py
├── cql/
│   └── schema.cql
├── hbase/
│   └── hbase_commands.txt
├── logs/
│   ├── 01_versions.txt
│   ├── 02_hdfs_files.txt
│   ├── 03_cassandra_tables.txt
│   └── assignment2_run_output.txt
└── screenshots/
    ├── 01_hdfs_files.png
    ├── 02_versions.png
    ├── 03_cassandra_connection.png
    ├── 04_cassandra_tables.png
    ├── 05_spark_cassandra_test.png
    ├── 06_task2_top10_movies.png
    ├── 07_task3_fav_genre.png
    ├── 08a_task4_users_under_20.png
    ├── 08b_task5_scientists_30_40.png
    ├── 09a_cassandra_top10_validation.png
    ├── 09b_cassandra_fav_genre_validation.png
    ├── 10_github_repo_structure.png
    ├── 11a_hbase_top10_movies.png
    └── 11b_hbase_fav_genres.png
```

---

## Notebook and Script

`assignment2_movielens.py` — main script, this is the one I actually ran with `spark-submit` in PuTTY.

`assignment2_notebook.ipynb` — same pipeline documented step by step with explanations and screenshots. Not meant to run in Colab — needs HDFS, Cassandra, HBase, and the Spark-Cassandra connector set up.

---

## Environment

Ran everything in Hortonworks Sandbox VM as `maria_dev`.

```text
Python        2.7.5
Apache Spark  2.3.0.2.6.5.0-292
Hadoop        2.7.3.2.6.5.0-292
Cassandra     3.0.x
CQLSH         5.0.1
HBase         1.1.2.2.6.5.0-292
Spark-Cassandra Connector  2.3.0
```

Version details in `logs/01_versions.txt` and `screenshots/02_versions.png`.

---

## How to Run

Most commands were run in PuTTY inside the Hortonworks Sandbox VM.

**0. Download and upload the dataset**

Download MovieLens 100k from:

```text
https://grouplens.org/datasets/movielens/
```

After downloading and unzipping it on my local computer, I used these three files only:

```text
u.data
u.item
u.user
```

I uploaded them into the VM through Ambari:

```text
Ambari → Files View → user → maria_dev → New Folder → ml-100k → Add
```

Then I opened the `ml-100k` folder and used `Upload` to upload `u.data`, `u.item`, and `u.user` from my local computer.

The final HDFS path is:

```text
/user/maria_dev/ml-100k/
```

Evidence is in `logs/02_hdfs_files.txt` and `screenshots/01_hdfs_files.png`.

**1. Check dataset in HDFS**

```bash
hdfs dfs -ls /user/maria_dev/ml-100k/
hdfs dfs -cat /user/maria_dev/ml-100k/u.data | head
hdfs dfs -cat /user/maria_dev/ml-100k/u.user | head
hdfs dfs -cat /user/maria_dev/ml-100k/u.item | head
```

**2. Start Cassandra and create tables**

```bash
service cassandra start
```

Test the connection first:

```bash
cqlsh 127.0.0.1 9042
```

Then create the keyspace and tables:

```bash
cqlsh 127.0.0.1 9042 -f cql/schema.cql
```

Do this before running Spark — if the tables are not there yet, the write step will fail.

Evidence in `screenshots/03_cassandra_connection.png`, `screenshots/04_cassandra_tables.png`, and `logs/03_cassandra_tables.txt`.

**3. Run the pipeline**

```bash
cd ~/assignment2

spark-submit \
  --master local[*] \
  --conf spark.eventLog.enabled=false \
  --conf spark.ui.showConsoleProgress=false \
  --packages com.datastax.spark:spark-cassandra-connector_2.11:2.3.0 \
  assignment2_movielens.py
```

Used `local[*]` because Spark and Cassandra were both on the same VM through `127.0.0.1:9042`.

Full output is in `logs/assignment2_run_output.txt`.

**4. Validate in Cassandra**

Cassandra tables were read back into Spark inside the script. I also checked manually in `cqlsh`:

```sql
USE movielens;
SELECT rank,movie_id,avg_rating,rating_count FROM top10_movies LIMIT 10;
SELECT user_id,total_ratings,favourite_genre,genre_count FROM user_favourite_genres LIMIT 10;
```

CQL does not return rows in ranked order, so Spark output is used for the actual results and CQL is just to confirm the data got stored.

Evidence in `screenshots/09a_cassandra_top10_validation.png` and `screenshots/09b_cassandra_fav_genre_validation.png`.

**5. HBase extension (optional)**

Check Ambari first — HMaster and RegionServer both need to be live.

```bash
hbase shell
```

Commands in `hbase/hbase_commands.txt`.

Evidence in `screenshots/11a_hbase_top10_movies.png` and `screenshots/11b_hbase_fav_genres.png`.

---

## Output Screenshots

Task i output is included in the notebook and the full terminal log. I did not keep a separate screenshot for Task i because the screenshot evidence focuses on ranking, genre analysis, filtering results, Cassandra validation, and HBase extension.

```text
06_task2_top10_movies.png              Task ii — top 10 movies
07_task3_fav_genre.png                 Task iii — favourite genre
08a_task4_users_under_20.png           Task iv — users under 20
08b_task5_scientists_30_40.png         Task v — scientist users 30–40
09a_cassandra_top10_validation.png     Cassandra read-back top10_movies
09b_cassandra_fav_genre_validation.png Cassandra read-back user_favourite_genres
10_github_repo_structure.png           GitHub repo structure
11a_hbase_top10_movies.png             HBase Task ii
11b_hbase_fav_genres.png               HBase Task iii
```

CQL does not return rows in ranked order, so Spark output is used for the actual results and CQL is just to confirm the data got stored.

---

## Things I Ran Into

- Cassandra has to be up before `spark-submit` — the connector connects at job start, not when it writes.
- Run `cql/schema.cql` first or the write step fails.
- If Cassandra is not running, Spark gives a connection error to `127.0.0.1:9042`.
- HBase shell kept throwing errors because RegionServer was not live. Fixed after checking Ambari properly.
- `spark.eventLog.enabled=false` stops the sandbox from spamming event log errors not related to the job.
