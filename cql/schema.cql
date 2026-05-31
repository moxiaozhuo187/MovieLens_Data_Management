-- Cassandra schema for MovieLens 100k

CREATE KEYSPACE IF NOT EXISTS movielens
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE movielens;

DROP TABLE IF EXISTS test_users;

CREATE TABLE IF NOT EXISTS average_movie_ratings (
    movie_id int PRIMARY KEY,
    title text,
    avg_rating double,
    rating_count int
);

CREATE TABLE IF NOT EXISTS top10_movies (
    rank int PRIMARY KEY,
    movie_id int,
    title text,
    avg_rating double,
    rating_count int
);

CREATE TABLE IF NOT EXISTS user_favourite_genres (
    user_id int PRIMARY KEY,
    age int,
    gender text,
    occupation text,
    zip text,
    total_ratings int,
    favourite_genre text,
    genre_count int
);

CREATE TABLE IF NOT EXISTS users_under_20 (
    user_id int PRIMARY KEY,
    age int,
    gender text,
    occupation text,
    zip text
);

CREATE TABLE IF NOT EXISTS scientists_30_40 (
    user_id int PRIMARY KEY,
    age int,
    gender text,
    occupation text,
    zip text
);

DESCRIBE TABLES;
