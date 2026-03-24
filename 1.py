import oracledb
import yaml
import sys

def load_config(file_path):
    try:
        with open(file_path, 'r') as file:
            return yaml.safe_load(file)
    except FileNotFoundError:
        print(f"Config file '{file_path}' not found")
        sys.exit(1)

config = load_config('config.yaml')

USER = config['db_user']
PASSWORD = config['db_passwd']
DSN = config['db_dns']

try: 
    with oracledb.connect(user=USER, password=PASSWORD, dsn=DSN) as conn:
        with conn.cursor() as cursor:
            sql = "SELECT table_name FROM all_tables WHERE owner = 'PAYMENT'"

            cursor.execute(sql)

            print("Список таблиць:")
            for row in cursor:
                print(f"- {row[0]}")

except oracledb.Error as e:
    print(f"Error: {e}")