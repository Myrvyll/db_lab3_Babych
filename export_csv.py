import psycopg2
import csv

username = 'iryna'
password = 'leaf'
database = 'lab2'
host = 'localhost'
port = '5432'


OUTPUT_FILE_T = 'babych_DB_lab3_output_{}.csv'

TABLES = [
    'penguinrecords',
    'islands',
    'studies'
]

connection = psycopg2.connect(user=username, password=password, dbname=database)

with connection:
    pointer = connection.cursor()

    for table_name in TABLES:
        pointer.execute('SELECT * FROM ' + table_name)

        fields = [x[0] for x in pointer.description]
        with open(OUTPUT_FILE_T.format(table_name), 'w') as outfile:
            writer = csv.writer(outfile)
            writer.writerow(fields)
            for row in pointer:
                writer.writerow([str(x) for x in row])
