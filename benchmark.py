import json
import pymysql
import time

def connect_to_database():
    """Connect to the MySQL database."""
    try:
        connection = pymysql.connect(
            host='mysql',
            user='root',
            password='my-secret-pw',
            database='imdb_db',
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor
        )
        return connection
    except pymysql.err.OperationalError as e:
        print(f"Failed to connect to MySQL: {e}")
        return None

def execute_query(connection, query):
    """Execute a single SQL query and measure execution time."""
    try:
        with connection.cursor() as cursor:
            start_time = time.time()
            cursor.execute(query)
            connection.commit()
            end_time = time.time()
        return end_time - start_time
    except pymysql.MySQLError as e:
        print(f"Error executing query: {query} with error {e}")
        return None

def run_queries(connection, queries, repetitions=20):
    """Run a list of queries multiple times, normalize, and calculate average run times."""
    results = {}
    all_times = []

    # First, collect all execution times to determine min and max for normalization
    for query in queries:
        query_times = []
        for _ in range(repetitions):
            execution_time = execute_query(connection, query)
            if execution_time is not None:
                query_times.append(execution_time)
        all_times.extend(query_times)
        results[query] = query_times

    if not all_times:  # Check if all_times is empty
        print("No successful query executions to normalize.")
        return {}

    # Normalize times based on min-max scaling
    min_time, max_time = min(all_times), max(all_times)
    for query, times in results.items():
        normalized_times = [(time - min_time) / (max_time - min_time) if max_time > min_time else 1.0 for time in times]
        average_time = sum(normalized_times) / len(normalized_times) if times else 'Failed to execute'
        results[query] = average_time

    return results


def main():
    # Connect to the database
    connection = connect_to_database()
    if connection is None:
        print("Database connection could not be established.")
        return

    # Queries for both JSON and Relational data
    queries = {
        "Relational": [
            "SELECT * FROM imdb_data WHERE primaryTitle = 'Carmencita';",
            "SELECT * FROM imdb_data WHERE runtimeMinutes BETWEEN 1 AND 10;",
            "SELECT * FROM imdb_data WHERE startYear = 1894;",
            "SELECT * FROM imdb_data WHERE startYear BETWEEN 1890 AND 1900;",
            "SELECT startYear, COUNT(*) AS titlesCount FROM imdb_data GROUP BY startYear;",
            "SELECT genres, MAX(runtimeMinutes) AS maxRuntime FROM imdb_data GROUP BY genres;",
            "SELECT a.tconst, a.primaryTitle, b.primaryTitle AS relatedTitle FROM imdb_data a JOIN imdb_data b ON a.tconst = b.tconst WHERE a.primaryTitle != b.primaryTitle;",
            "SELECT a.tconst AS OriginalTconst, a.primaryTitle AS OriginalTitle, a.startYear AS OriginalYear,  b.tconst AS LatestTconst, b.primaryTitle AS LatestTitle, b.startYear AS LatestYear FROM imdb_data a JOIN imdb_data b ON a.primaryTitle = b.primaryTitle AND a.tconst <> b.tconst WHERE a.startYear < b.startYear ORDER BY a.primaryTitle, a.startYear;",
            "(SELECT primaryTitle FROM imdb_data WHERE startYear = 1894) UNION (SELECT primaryTitle FROM imdb_data WHERE startYear = 1892);",
            "SELECT DISTINCT a.primaryTitle FROM imdb_data a JOIN imdb_data b ON a.primaryTitle = b.primaryTitle WHERE a.startYear = 1894 AND b.startYear = 1892;",
            "SELECT primaryTitle FROM imdb_data WHERE startYear = 1894 AND primaryTitle NOT IN (SELECT primaryTitle FROM imdb_data WHERE startYear = 1892);",
            "SELECT * FROM imdb_data ORDER BY runtimeMinutes DESC;",
            "SELECT * FROM imdb_data ORDER BY startYear ASC;",
            "SELECT DISTINCT genres FROM imdb_data;",
            "SELECT a.tconst, a.primaryTitle, b.primaryTitle AS relatedTitle1, c.primaryTitle AS relatedTitle2 FROM imdb_data a JOIN imdb_data b ON a.tconst = b.tconst JOIN imdb_data c ON a.tconst = c.tconst WHERE a.primaryTitle != b.primaryTitle OR a.primaryTitle != c.primaryTitle;",
            "SELECT a.tconst, a.primaryTitle, IFNULL(b.primaryTitle, 'No related title') AS relatedTitle FROM imdb_data a LEFT JOIN imdb_data b ON a.tconst = b.tconst AND a.startYear != b.startYear;",
            "SELECT genres, COUNT(*) AS titlesCount, MAX(runtimeMinutes) AS maxRuntime FROM imdb_data GROUP BY genres;"
        ],
        "JSON": [
            "SELECT * FROM imdb_json_data WHERE JSON_UNQUOTE(JSON_EXTRACT(json_data, '$.primaryTitle')) = 'Carmencita';",
            "SELECT * FROM imdb_json_data WHERE JSON_EXTRACT(json_data, '$.runtimeMinutes') BETWEEN 1 AND 10;",
            "SELECT * FROM imdb_json_data WHERE JSON_UNQUOTE(JSON_EXTRACT(json_data, '$.startYear')) = '1894';",
            "SELECT * FROM imdb_json_data WHERE JSON_UNQUOTE(JSON_EXTRACT(json_data, '$.startYear')) BETWEEN '1890' AND '1900';",
            "SELECT JSON_EXTRACT(json_data, '$.startYear') AS Year, COUNT(*) AS Count FROM imdb_json_data GROUP BY JSON_EXTRACT(json_data, '$.startYear');",
            "SELECT JSON_EXTRACT(json_data, '$.genres') AS Genre, MAX(CAST(JSON_EXTRACT(json_data, '$.runtimeMinutes') AS UNSIGNED)) AS MaxRuntime FROM imdb_json_data GROUP BY JSON_EXTRACT(json_data, '$.genres');",
            "SELECT a.json_data->>'$.primaryTitle' AS TitleA, b.json_data->>'$.primaryTitle' AS TitleB FROM imdb_json_data a, imdb_json_data b WHERE a.json_data->>'$.tconst' = b.json_data->>'$.tconst' AND a.json_data->>'$.primaryTitle' != b.json_data->>'$.primaryTitle';",
            "(SELECT json_data->>'$.primaryTitle' AS Title FROM imdb_json_data WHERE JSON_UNQUOTE(json_data->'$.startYear') = '1894') UNION (SELECT json_data->>'$.primaryTitle' FROM imdb_json_data WHERE JSON_UNQUOTE(json_data->'$.startYear') = '1892');",
            "SELECT DISTINCT a.json_data->>'$.primaryTitle' FROM imdb_json_data a JOIN imdb_json_data b ON a.json_data->>'$.primaryTitle' = b.json_data->>'$.primaryTitle' WHERE JSON_UNQUOTE(a.json_data->'$.startYear') = '1894' AND JSON_UNQUOTE(b.json_data->'$.startYear') = '1892';",
            "SELECT json_data->>'$.primaryTitle' FROM imdb_json_data WHERE JSON_UNQUOTE(json_data->'$.startYear') = '1894' AND json_data->>'$.primaryTitle' NOT IN (SELECT json_data->>'$.primaryTitle' FROM imdb_json_data WHERE JSON_UNQUOTE(json_data->'$.startYear') = '1892');", 
            "SELECT * FROM imdb_json_data ORDER BY CAST(json_data->>'$.runtimeMinutes' AS UNSIGNED) DESC;",
            "SELECT * FROM imdb_json_data ORDER BY JSON_UNQUOTE(json_data->'$.startYear') ASC;",
            "SELECT DISTINCT JSON_UNQUOTE(json_data->'$.genres') FROM imdb_json_data;",
            "SELECT JSON_EXTRACT(json_data, '$.genres') AS Genre, COUNT(*) AS Count FROM imdb_json_data GROUP BY JSON_EXTRACT(json_data, '$.genres') ORDER BY Count DESC;",
            "SELECT DISTINCT a.json_data->>'$.primaryTitle' FROM imdb_json_data a WHERE JSON_UNQUOTE(a.json_data->'$.startYear') = '1894' AND a.json_data->>'$.primaryTitle' IN (SELECT b.json_data->>'$.primaryTitle' FROM imdb_json_data b WHERE JSON_UNQUOTE(b.json_data->'$.startYear') = '1892');",
            "SELECT JSON_UNQUOTE(a.json_data->'$.primaryTitle') AS TitleA, IFNULL(JSON_UNQUOTE(b.json_data->'$.primaryTitle'), 'No related title') AS TitleB FROM imdb_json_data a LEFT JOIN imdb_json_data b ON JSON_EXTRACT(a.json_data, '$.tconst') = JSON_EXTRACT(b.json_data, '$.tconst') AND JSON_EXTRACT(a.json_data, '$.startYear') != JSON_EXTRACT(b.json_data, '$.startYear') WHERE JSON_UNQUOTE(a.json_data->'$.startYear') = '1894';",
            "SELECT JSON_EXTRACT(json_data, '$.genres') AS Genre, COUNT(*) AS Count, MAX(CAST(JSON_EXTRACT(json_data, '$.runtimeMinutes') AS UNSIGNED)) AS MaxRuntime FROM imdb_json_data GROUP BY JSON_EXTRACT(json_data, '$.genres');",
            "SELECT JSON_UNQUOTE(JSON_EXTRACT(a.json_data, '$.primaryTitle')) AS TitleA, JSON_UNQUOTE(JSON_EXTRACT(a.json_data, '$.startYear')) AS YearA, JSON_UNQUOTE(JSON_EXTRACT(b.json_data, '$.primaryTitle')) AS TitleB, JSON_UNQUOTE(JSON_EXTRACT(b.json_data, '$.startYear')) AS YearB FROM imdb_json_data a JOIN imdb_json_data b ON JSON_UNQUOTE(JSON_EXTRACT(a.json_data, '$.primaryTitle')) = JSON_UNQUOTE(JSON_EXTRACT(b.json_data, '$.primaryTitle'))  AND JSON_UNQUOTE(JSON_EXTRACT(a.json_data, '$.tconst')) <> JSON_UNQUOTE(JSON_EXTRACT(b.json_data, '$.tconst')) AND JSON_UNQUOTE(JSON_EXTRACT(a.json_data, '$.startYear')) < JSON_UNQUOTE(JSON_EXTRACT(b.json_data, '$.startYear')) ORDER BY JSON_UNQUOTE(JSON_EXTRACT(a.json_data, '$.primaryTitle')), JSON_UNQUOTE(JSON_EXTRACT(a.json_data, '$.startYear'));"
        ]
    }

    # Run queries and calculate average normalized times
    results = {}
    for category, queries_list in queries.items():
        print(f"\nRunning queries for {category}:")
        results[category] = run_queries(connection, queries_list)
        for query, avg_time in results[category].items():
            print(f"\nQuery: {query}\nNormalized Average Execution Time: {avg_time if isinstance(avg_time, str) else f'{avg_time:.5f} (normalized scale)'}")

    # Close the database connection
    connection.close()

if __name__ == "__main__":
    main()
