
import psycopg2, psycopg2.extras
from config import config

class DBProvider():
    def __init__(self):
        self.host = config.Host
        self.user = config.User
        self.password = config.Password
        self.database_name = config.Database

    def __prepare_value(self, value):
        if value is None:
            return 'NULL'
        if isinstance(value, str):
            return f"'{value}'"
        return str(value)

    def join_values(self, values):
        return ', '.join([self.__prepare_value(value) for value in values])

    def execute(self, executor):
        try:
            conn = psycopg2.connect(dbname=self.database_name, user=self.user, password=self.password, host=self.host)
            result = executor(conn)
            conn.commit()
            return result
        finally:
            if conn:
                conn.close()

    def select(self, request):
        def executor(conn):
            cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
            cursor.execute(request)
            return cursor.fetchall()
        return self.execute(executor)

    def insert_matches(self, table_name, league_id, matches):
        int_league_id = self.select(f"SELECT id FROM current_leagues WHERE league_id = '{league_id}'")
        int_league_id = int_league_id[0]['id']
        inserted_matches = []
        for match_info in matches:
            columns = ', '.join(match_info.keys())
            values = self.join_values(match_info.values())
            def insert_executor(conn):
                cursor = conn.cursor()
                insert_match_request = f"INSERT INTO {table_name} (league_id, {columns}) VALUES ({int_league_id}, {values}) RETURNING match_id;"
                cursor.execute(insert_match_request)
                return cursor.fetchone()[0]
            inserted = self.execute(insert_executor)
            inserted_matches.append(inserted)
        return inserted_matches