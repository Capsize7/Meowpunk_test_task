import csv
import sqlite3
from datetime import datetime
from memory_profiler import memory_usage

def create_empty_table():
    table_query = '''CREATE TABLE IF NOT EXISTS result
        (
            timestamp integer,
            player_id text,
            event_id text,
            error_id text,
            json_server text,
            json_client text
        )'''

    connection = sqlite3.connect('cheaters.db')
    cursor = connection.cursor()
    cursor.execute(table_query)

def create_table_from_csv(date, name):
    with open(name, 'r') as csvfile:
        csv_file_reader = csv.DictReader(csvfile, delimiter=',')
        header_1, header_2, header_3, header_4 = csv_file_reader.fieldnames

        table_query = f'CREATE TABLE if not Exists {name[:-4]} \
        ({header_1} integer, {header_2} text, {header_3} text, {header_4} TEXT)'

        connection = sqlite3.connect('cheaters.db')
        cursor = connection.cursor()
        cursor.execute(table_query)

        for row in csv_file_reader:
            if datetime.fromtimestamp(int(row['timestamp'])).date().isoformat() == date:
                InsertQuery = (f"INSERT INTO {name[:-4]} VALUES ('{(row[header_1])}', '{row[header_2]}',"
                               f"'{row[header_3]}','{row[header_4]}')")
                cursor.execute(InsertQuery)
        connection.commit()
        connection.close()

def create_table():
    table_query = '''INSERT INTO result(timestamp, player_id, event_id, error_id, json_server, json_client)
    SELECT s.timestamp, c.player_id, s.event_id, error_id, s.description, c.description
    FROM server as s
    JOIN client as c USING(error_id)
    WHERE c.player_id not in (SELECT player_id FROM cheaters) or
    s.timestamp - (SELECT unixepoch(ban_time) FROM cheaters where c.player_id = player_id) < 86400'''
    
    connection = sqlite3.connect('cheaters.db')
    cursor = connection.cursor()
    cursor.execute(table_query)
    connection.commit()
    connection.close()

if __name__ == '__main__':
    date = '2021-01-05' # example, change to your needed date in format: yyyy-mm-dd
    mem_usage_1 = memory_usage(create_empty_table)
    mem_usage_2 = memory_usage((create_table_from_csv, (date, 'server.csv')))
    mem_usage_3 = memory_usage((create_table_from_csv, (date, 'client.csv')))
    mem_usage_4 = memory_usage(create_table)
    print(f'The maximum of memory usage is '
          f'{round(max(mem_usage_1)+max(mem_usage_2)+max(mem_usage_3)+max(mem_usage_4), 2)} MiB')

