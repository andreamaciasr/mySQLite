# mySQLite

**mySQLite** is a work-in-progress Java project that implements a simple SQL-like query engine. It currently supports basic SQL operations including `SELECT`, `FROM`, `JOIN`, `ON`, and `WHERE` clauses, allowing queries across two different tables.

## Features

- Parse and execute queries with `SELECT`, `FROM`, `JOIN`, `ON`, and `WHERE`
- Supports joining two tables
- Database files must be located inside the project directory (The project contains two sample database files and one database of the top 1000 movies from imdb website)
- Full file paths must be provided in the `FROM` and `JOIN` clauses

## Usage

1. Place your database files inside the project directory.
2. When writing queries, provide the full path to the database files in the `FROM` and `JOIN` clauses.

**Example queries:**

SELECT * FROM test_data/imdb_top_1000.csv WHERE imdb_top_1000.Released_Year = '1994'
SELECT movies_demo.title, movies_demo.genre  FROM test_data/movies_demo.csv  JOIN test_data/directors_demo.csv ON movies_demo.director_id = directors_demo.id WHERE directors_demo.name = "Greta Gerwig"