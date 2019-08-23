"""
    Python code snippets to work with IBM PostgreSQL and the `psycopg2` library.
    psycopg2 can be installed using the command: `pip install psycopg2`.
    Updated the last time at: 00:46 23/AUG/2019 by @vnderlev
"""

import json
import base64
import psycopg2
from psycopg2 import pool

# Copy the IPSQL credentials from the IBM Cloud Web page and save them as a json file, as named below. 
IPSQL_JSONFILE = "ipsql_credentials.json"
with open(IPSQL_JSONFILE) as json_file:
    ipsql_cred = json.load(json_file)

# Build the root certificate file:
with open("postgresql.crt", "w") as rootcert:
    coded_cert = ipsql_cred['connection']['postgres']['certificate']['certificate_base64']
    rootcert.write(base64.b64decode(coded_cert).decode('utf-8'))

# Build the root certificate key file:
with open("postgresql.key", "w") as key:
    key.write(ipsql_cred['connection']['postgres']['certificate']['name'])

# Build a psql credentials dictionary to be used by psycopg2:
ipsql = {
    "dbname": ipsql_cred['connection']['postgres']['database'],
    "user": ipsql_cred['connection']['postgres']['authentication']['username'],
    "password": ipsql_cred['connection']['postgres']['authentication']['password'],
    "host": ipsql_cred['connection']['postgres']['hosts'][0]['hostname'],
    "port" : ipsql_cred['connection']['postgres']['hosts'][0]['port'],
    "sslmode": ipsql_cred['connection']['postgres']['query_options']['sslmode'],
    "sslkey": "postgresql.key",
    "sslrootcert": "postgresql.crt" 
}


# Try to establish connection with the postgresql db:
print("\nTrying to establish connection with ipsql...")
conn = None
try:
    conn = psycopg2.connect(**ipsql)
    curs = conn.cursor()
    # Print postgresql db connection properties:
    print("\nPostgreSQL Connection Properties:\n{}".format(
        json.dumps(conn.get_dsn_parameters(), 
        indent=3, sort_keys=False))
    )
    # Print postgresql db version:
    curs.execute("SELECT version();")
    record = curs.fetchone()
    print("\nYou are connected to {}".format(record))
except (Exception, psycopg2.Error) as error:
    print("\nException: {}".format(error))
finally:
    # Closing the postgresql db connection:
    if conn:
        curs.close()
        conn.close()
        print("\nPostgreSQL DB connection closed.\n")


# Try creating a connection pool using the psycopg2 pool classes:
print("\nTrying to create a connection pool with ipsql...")
psql_simple_pool = None
try:
    # Create a simple connection pool (for a threaded connection pool use `ThreadedConnectionPool` instead):
    psql_simple_pool = psycopg2.pool.SimpleConnectionPool(1, 20, **ipsql)
except (Exception, psycopg2.Error) as error:
    print("\nException: {}".format(error))
finally:
    if psql_simple_pool:
        print("\nConnection pool `psql_simple_pool` created successfully.")


# Try to establish connection with the postgresql db using a connection pool:
print("\nTrying to get a connection from the ipsql connection pool...")
simple_pool_conn = None
try:
    simple_pool_conn = psql_simple_pool.getconn()
    sp_cursor = simple_pool_conn.cursor()
    # Print postgresql db connection properties:
    print("\nPostgreSQL Connection Properties:\n{}".format(
        json.dumps(simple_pool_conn.get_dsn_parameters(), 
        indent=3, sort_keys=False))
    )
    # Print postgresql db version:
    sp_cursor.execute("SELECT version();")
    record = sp_cursor.fetchone()
    print("\nYou are connected to {}.".format(record))
except (Exception, psycopg2.Error) as error:
    print("\nException: {}".format(error))
finally:
    # Closing the postgresql db connection:
    if simple_pool_conn:
        sp_cursor.close()
        #Use the next method to release the connection object and send it back to connection pool
        psql_simple_pool.putconn(simple_pool_conn)
        print("\nipsql connection returned to the connection pool.\n")


# Shutting down the connection pool:
print("\nShutting down the ipsql connection pool...")
try:
    psql_simple_pool.closeall()
except (Exception, psycopg2.Error) as error:
    print("\nException: {}".format(error))
finally:
    print("\nPostgreSQL connection pool `psql_simple_pool` is closed.")


""" 
''' Some CRUD examples of sql queries with psycopg2... '''

# Creating a new table at the ipsql db:
sql = '''CREATE TABLE my_table_name (ID INT PRIMARY KEY NOT NULL, MODEL TEXT NOT NULL, PRICE REAL);'''
curs.execute(sql)
conn.commit()

# Running a simple SELECT query:
sql = '''SELECT * FROM my_table_name;'''
curs.execute(sql)
records = curs.fetchmany(2)
print("\nDisplaying rows from `my_table_name` table...")
for row in records:
    print(row)
# Working with pandas:
import numpy
import pandas
sql = '''SELECT * FROM my_table_name;'''
df = pandas.read_sql_query(sql, conn)
print("\nDisplaying rows form `my_table_name` table...".format(df.tail()))

# Running a simple INSERT query:
sql = '''INSERT INTO my_table_name (ID, MODEL, PRICE) VALUES (%s,%s,%s)'''
record_to_insert = (5, 'One Plus 6', 950)
curs.execute(sql, record_to_insert)
conn.commit()
print("{} records inserted successfully into `my_table_name` table".format(curs.rowcount))

# Running a simple UPDATE query:
sql = '''UPDATE my_table_name SET price = %s WHERE id = %s'''
curs.execute(sql, (price, id))
conn.commit()
print("{} records updated successfully at `my_table_name` table".format(curs.rowcount))

# Running a simple DELETE query:
sql = '''Delete from mobile where id = %s'''
curs.execute(sql, (id, ))
conn.commit()
print("{} records deleted successfully from `my_table_name` table".format(curs.rowcount))

# Using `Cursor.executemany()` to CRUD multiple rows into a psql table
# Example with INSERT query:
cursor = connection.cursor()
sql = '''INSERT INTO my_table_name (id, model, price) VALUES (%s,%s,%s)'''
records_to_insert = [ (4,'LG', 800) , (5,'One Plus 6', 950)]
curs.executemany(sql, records_to_insert)
conn.commit()
print("{} records inserted successfully into `my_table_name` table".format(curs.rowcount))

"""