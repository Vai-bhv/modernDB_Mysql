import json
import pymysql
import subprocess
import time

def connect_with_retry(host, user, password, database, retries=5, delay=5):
    """Attempt to connect to MySQL database with retries."""
    connection = None
    while retries > 0:
        try:
            connection = pymysql.connect(host=host,
                                         user=user,
                                         password=password,
                                         database=database,
                                         charset='utf8mb4',
                                         cursorclass=pymysql.cursors.DictCursor)
            print("MySQL database connection was successful")
            break
        except pymysql.err.OperationalError as e:
            print(f"Failed to connect to MySQL: {e}, retries left: {retries}")
            retries -= 1
            time.sleep(delay)
            if retries == 0:
                raise Exception("Failed to connect to MySQL after several retries")
    return connection

def data_already_loaded(connection):
    """Check if the data is already loaded into the database."""
    with connection.cursor() as cursor:
        cursor.execute("SELECT COUNT(*) AS count FROM imdb_json_data")
        result = cursor.fetchone()
        return result['count'] > 500000

# MySQL connection setup
host = 'mysql'  # Use the service name defined in docker-compose
user = 'root'
password = 'my-secret-pw'
database = 'imdb_db'

# Try to establish a connection with retries
connection = connect_with_retry(host, user, password, database)

try:
    if not data_already_loaded(connection):
        with connection:
            with connection.cursor() as cursor:
                # Open and load JSON file
                with open('filtered_titled_basics.json', 'r') as file:
                    data = json.load(file)
                    sql = '''
                    INSERT INTO imdb_json_data (tconst, json_data)
                    VALUES (%s, %s)
                    ON DUPLICATE KEY UPDATE
                    json_data = VALUES(json_data);
                    '''

                    for tconst, item in data.items():
                        # Prepare JSON data
                        json_data = json.dumps({
                            "titleType": item['titleType'],
                            "primaryTitle": item['primaryTitle'],
                            "originalTitle": item['originalTitle'],
                            "isAdult": item['isAdult'],
                            "startYear": item['startYear'] if item['startYear'] != '\\N' else None,
                            "endYear": item['endYear'] if item['endYear'] != '\\N' else None,
                            "runtimeMinutes": item['runtimeMinutes'] if item['runtimeMinutes'] != '\\N' else None,
                            "genres": item['genres']
                        })
                        cursor.execute(sql, (tconst, json_data))
                connection.commit()

            load_tsv_command = f'''
                mysql -h {host} -u {user} --password={password} -e "
                USE {database};
                LOAD DATA INFILE '/var/lib/mysql-files/filtered_title.basics.tsv'
                INTO TABLE imdb_data
                FIELDS TERMINATED BY '\\t'
                OPTIONALLY ENCLOSED BY '\\"'
                LINES TERMINATED BY '\\n'
                IGNORE 1 ROWS
                (tconst, titleType, primaryTitle, originalTitle, isAdult, startYear, @endYear, @runtimeMinutes, genres)
                SET
                  endYear = NULLIF(@endYear, '\\\\N'),  -- Convert '\\N' to NULL for endYear
                  runtimeMinutes = NULLIF(@runtimeMinutes, '\\\\N');  -- Convert '\\N' to NULL for runtimeMinutes
                "
                '''
            subprocess.run(load_tsv_command, shell=True)
        print("Data has been loaded successfully into the MySQL database!")
    else:
        print("Data is already loaded. No action taken.")

finally:
    if connection.open:
        connection.close()
