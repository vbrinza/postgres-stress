import psycopg2
import datetime
import os

try:
    connection = psycopg2.connect(user=os.environ["DB_USER"],
                                  password=os.environ["DB_PASSWORD"],
                                  host=os.environ["DB_HOST"],
                                  port=os.environ["DB_PORT"],
                                  database=os.environ["DB_DATABASE"])

    cursor = connection.cursor()
    cursor.execute("SELECT version();")
    record = cursor.fetchone()
    print(f"You are connected to {record}")
    create_table_query = '''CREATE TABLE IF NOT EXISTS entry
        (ID INT PRIMARY KEY NOT NULL,
        SUBJECT TEXT NOT NULL,
        BODY TEXT,
        POSTING_DATE TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP );'''
    cursor.execute(create_table_query)
    connection.commit()
    insert_query = '''INSERT INTO entry (ID, SUBJECT, BODY) VALUES(%s,%s,%s);'''
    print((os.environ["WRITE_OFFSET"]))
    for increment_id in range(int(os.environ["WRITE_OFFSET_START"]), int(os.environ["WRITE_OFFSET_END"])):
        record_to_insert = (increment_id, 'Subject ' + str(increment_id), 'Body ' + str(increment_id))
        print(datetime.datetime.now(), record_to_insert)
        try:
            cursor.execute(insert_query, record_to_insert)
        except Exception as err:
            print(f"Exception: {err}")
            print(f"Exception type: {type(err)}")
        connection.commit()
except (Exception, psycopg2.Error) as error:
    print(f"error while connecting to postgres: {error}")
finally:
    if connection:
        cursor.close()
        connection.close()
