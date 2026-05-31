from __future__ import print_function

#1.import libraries
from pyspark.sql import SparkSession, Row
from pyspark.sql import functions as fn
from pyspark.sql.window import Window

try:
    unicode
except NameError:
    unicode = str


#2.basic path
hdfs_path = "hdfs:///user/maria_dev/ml-100k"
ks = "movielens"

rate_path = hdfs_path + "/u.data"
mv_path = hdfs_path + "/u.item"
usr_path = hdfs_path + "/u.user"

genre_lst = [
    "unknown", "Action", "Adventure", "Animation", "Children",
    "Comedy", "Crime", "Documentary", "Drama", "Fantasy",
    "Film-Noir", "Horror", "Musical", "Mystery", "Romance",
    "Sci-Fi", "Thriller", "War", "Western"
]


#3.fix text
def fix_txt(x):
    if x is None:
        return ""

    try:
        if isinstance(x, unicode):
            return x
        return x.decode("ISO-8859-1", "ignore")
    except:
        return str(x)


#4.get rating row from u.data
def get_rate(line):
    try:
        p = line.split("\t")

        return Row(
            user_id=int(p[0]),
            movie_id=int(p[1]),
            rating=float(p[2]),
            ts=int(p[3])
        )
    except:
        return None


#5.get movie row from u.item
def get_mv(line):
    try:
        p = line.split("|")

        return Row(
            movie_id=int(p[0]),
            title=fix_txt(p[1])
        )
    except:
        return None


#6.get user row from u.user
def get_usr(line):
    try:
        p = line.split("|")

        return Row(
            user_id=int(p[0]),
            age=int(p[1]),
            gender=fix_txt(p[2]),
            occupation=fix_txt(p[3]),
            zip=fix_txt(p[4])
        )
    except:
        return None


#7.get movie genre row
def get_mg(line):
    out = []

    try:
        p = line.split("|")
        mid = int(p[0])
        ttl = fix_txt(p[1])

        for i in range(len(genre_lst)):
            idx = 5 + i

            if p[idx].strip() == "1":
                out.append(Row(movie_id=mid, title=ttl, genre=genre_lst[i]))

        if len(out) == 0:
            out.append(Row(movie_id=mid, title=ttl, genre="unknown"))

    except:
        pass

    return out


#8.start spark
spark = SparkSession.builder \
    .appName("a2_ml_spark_cassandra") \
    .config("spark.cassandra.connection.host", "127.0.0.1") \
    .config("spark.cassandra.connection.port", "9042") \
    .getOrCreate()

sc = spark.sparkContext
sc.setLogLevel("ERROR")
print("\nstart assignment 2 pipeline")
print("spark version:", spark.version)
print("hdfs path:", hdfs_path)
print("cassandra keyspace:", ks)


#9.read raw files from hdfs
print("\nread raw files from hdfs")

rate_raw = sc.textFile(rate_path)
mv_raw = sc.textFile(mv_path)
usr_raw = sc.textFile(usr_path)

print("u.data rows:", rate_raw.count())
print("u.item rows:", mv_raw.count())
print("u.user rows:", usr_raw.count())


#10.parse rdd
print("\nparse rdd")

rate_rdd = rate_raw.map(get_rate).filter(lambda x: x is not None)
mv_rdd = mv_raw.map(get_mv).filter(lambda x: x is not None)
usr_rdd = usr_raw.map(get_usr).filter(lambda x: x is not None)
mg_rdd = mv_raw.flatMap(get_mg)


#11.make dataframe
print("\nmake dataframe")

rate_df = spark.createDataFrame(rate_rdd)
mv_df = spark.createDataFrame(mv_rdd)
usr_df = spark.createDataFrame(usr_rdd)
mg_df = spark.createDataFrame(mg_rdd)

print("\nrate_df schema")
rate_df.printSchema()

print("\nmv_df schema")
mv_df.printSchema()

print("\nusr_df schema")
usr_df.printSchema()

print("\nmg_df schema")
mg_df.printSchema()


#12.clean data
print("\nclean data")

rate_df = rate_df.dropna().dropDuplicates() \
    .filter((fn.col("user_id") >= 1) &
            (fn.col("movie_id") >= 1) &
            (fn.col("rating") >= 1) &
            (fn.col("rating") <= 5))

mv_df = mv_df.dropna().dropDuplicates(["movie_id"]) \
    .filter(fn.col("movie_id") >= 1)

usr_df = usr_df.dropna().dropDuplicates(["user_id"]) \
    .filter((fn.col("user_id") >= 1) & (fn.col("age") > 0))

mg_df = mg_df.dropna().dropDuplicates(["movie_id", "genre"]) \
    .filter(fn.col("movie_id") >= 1)

#keep only valid user and valid movie
rate_df = rate_df.join(usr_df.select("user_id"), "user_id", "inner") \
    .join(mv_df.select("movie_id"), "movie_id", "inner")

rate_df.cache()
mv_df.cache()
usr_df.cache()
mg_df.cache()

print("clean rating:", rate_df.count())
print("clean movie:", mv_df.count())
print("clean user:", usr_df.count())
print("movie genre rows:", mg_df.count())

print("\nsample rating")
rate_df.show(5, False)

print("\nsample movie")
mv_df.show(5, False)

print("\nsample user")
usr_df.show(5, False)


#13.make temp views
rate_df.createOrReplaceTempView("rate")
mv_df.createOrReplaceTempView("mv")
usr_df.createOrReplaceTempView("usr")
mg_df.createOrReplaceTempView("mg")


#14.task 1: average rating for each movie
print("\ntask 1: average rating for each movie")

avg_df = rate_df.groupBy("movie_id") \
    .agg(
        fn.round(fn.avg("rating"), 4).alias("avg_rating"),
        fn.count("rating").cast("int").alias("rating_count")
    ) \
    .join(mv_df, "movie_id", "left") \
    .select("movie_id", "title", "avg_rating", "rating_count") \
    .orderBy("movie_id")

avg_df.show(10, False)


#15.task 2: top 10 movies
print("\ntask 2: top 10 movies by average rating")

top_w = Window.orderBy(
    fn.desc("avg_rating"),
    fn.desc("rating_count"),
    fn.asc("title")
)

top10_df = avg_df.withColumn("rank", fn.row_number().over(top_w)) \
    .filter(fn.col("rank") <= 10) \
    .select("rank", "movie_id", "title", "avg_rating", "rating_count") \
    .orderBy("rank")

top10_df.show(10, False)


#16.task 3: favourite genre
print("\ntask 3: favourite genre for users with at least 50 ratings")

act_df = rate_df.groupBy("user_id") \
    .agg(fn.count("*").cast("int").alias("total_ratings")) \
    .filter(fn.col("total_ratings") >= 50)

rm_df = rate_df.join(
    mg_df.select("movie_id", "genre"),
    "movie_id",
    "inner"
)

ug_df = rm_df.groupBy("user_id", "genre") \
    .agg(fn.count("*").cast("int").alias("genre_count"))

g_w = Window.partitionBy("user_id") \
    .orderBy(fn.desc("genre_count"), fn.asc("genre"))

fav_df = ug_df.withColumn("grank", fn.row_number().over(g_w)) \
    .filter(fn.col("grank") == 1) \
    .drop("grank") \
    .join(act_df, "user_id", "inner") \
    .join(usr_df, "user_id", "left") \
    .select(
        "user_id",
        "age",
        "gender",
        "occupation",
        "zip",
        "total_ratings",
        fn.col("genre").alias("favourite_genre"),
        "genre_count"
    ) \
    .orderBy("user_id")

fav_df.show(20, False)


#17.task 4: users below 20
print("\ntask 4: users less than 20 years old")

u20_df = spark.sql("""
    SELECT user_id, age, gender, occupation, zip
    FROM usr
    WHERE age < 20
    ORDER BY user_id
""")

print("number of users under 20:", u20_df.count())
u20_df.show(20, False)


#18.task 5: scientist age 30 to 40
print("\ntask 5: scientist users aged 30 to 40")

sci_df = spark.sql("""
    SELECT user_id, age, gender, occupation, zip
    FROM usr
    WHERE lower(occupation) = 'scientist'
      AND age >= 30
      AND age <= 40
    ORDER BY user_id
""")

print("number of scientist age 30-40:", sci_df.count())
sci_df.show(50, False)


#19.write result to cassandra
print("\nwrite result to cassandra")

#make cassandra type match
avg_cas = avg_df.select(
    fn.col("movie_id").cast("int").alias("movie_id"),
    "title",
    fn.col("avg_rating").cast("double").alias("avg_rating"),
    fn.col("rating_count").cast("int").alias("rating_count")
)

top10_cas = top10_df.select(
    fn.col("rank").cast("int").alias("rank"),
    fn.col("movie_id").cast("int").alias("movie_id"),
    "title",
    fn.col("avg_rating").cast("double").alias("avg_rating"),
    fn.col("rating_count").cast("int").alias("rating_count")
)

fav_cas = fav_df.select(
    fn.col("user_id").cast("int").alias("user_id"),
    fn.col("age").cast("int").alias("age"),
    "gender",
    "occupation",
    "zip",
    fn.col("total_ratings").cast("int").alias("total_ratings"),
    "favourite_genre",
    fn.col("genre_count").cast("int").alias("genre_count")
)

u20_cas = u20_df.select(
    fn.col("user_id").cast("int").alias("user_id"),
    fn.col("age").cast("int").alias("age"),
    "gender",
    "occupation",
    "zip"
)

sci_cas = sci_df.select(
    fn.col("user_id").cast("int").alias("user_id"),
    fn.col("age").cast("int").alias("age"),
    "gender",
    "occupation",
    "zip"
)

avg_cas.write.format("org.apache.spark.sql.cassandra") \
    .mode("append") \
    .options(table="average_movie_ratings", keyspace=ks) \
    .save()

top10_cas.write.format("org.apache.spark.sql.cassandra") \
    .mode("append") \
    .options(table="top10_movies", keyspace=ks) \
    .save()

fav_cas.write.format("org.apache.spark.sql.cassandra") \
    .mode("append") \
    .options(table="user_favourite_genres", keyspace=ks) \
    .save()

u20_cas.write.format("org.apache.spark.sql.cassandra") \
    .mode("append") \
    .options(table="users_under_20", keyspace=ks) \
    .save()

sci_cas.write.format("org.apache.spark.sql.cassandra") \
    .mode("append") \
    .options(table="scientists_30_40", keyspace=ks) \
    .save()

print("write done")


#20.read back from cassandra
print("\nread back from cassandra for checking")

chk_avg = spark.read.format("org.apache.spark.sql.cassandra") \
    .options(table="average_movie_ratings", keyspace=ks) \
    .load() \
    .orderBy("movie_id")

chk_top10 = spark.read.format("org.apache.spark.sql.cassandra") \
    .options(table="top10_movies", keyspace=ks) \
    .load() \
    .orderBy("rank")

chk_fav = spark.read.format("org.apache.spark.sql.cassandra") \
    .options(table="user_favourite_genres", keyspace=ks) \
    .load() \
    .orderBy("user_id")

chk_u20 = spark.read.format("org.apache.spark.sql.cassandra") \
    .options(table="users_under_20", keyspace=ks) \
    .load() \
    .orderBy("user_id")

chk_sci = spark.read.format("org.apache.spark.sql.cassandra") \
    .options(table="scientists_30_40", keyspace=ks) \
    .load() \
    .orderBy("user_id")

print("\ncheck average_movie_ratings")
chk_avg.show(10,False)

print("\ncheck top10_movies")
chk_top10.show(10, False)

print("\ncheck user_favourite_genres")
chk_fav.show(20, False)

print("\ncheck users_under_20")
chk_u20.show(20, False)

print("\ncheck scientists_30_40")
chk_sci.show(50, False)


#21.short discussion
print("\nDiscussion")
print("task 1 calculates average rating and rating count for each movie.")
print("task 2 ranks movies by average rating. rating_count is used when average rating ties.")
print("task 3 uses users with at least 50 ratings and finds the genre they rated most often.")
print("for multi-genre movies, one rating is counted once for each genre.")
print("task 4 filters users whose age is below 20.")
print("task 5 filters scientist users aged from 30 to 40.")
print("all result dataframes are written into cassandra and read back for checking.")

print("\nfinished")

spark.stop()
