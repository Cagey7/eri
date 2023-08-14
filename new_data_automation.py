from datetime import datetime, timedelta 
import psycopg2
import requests
from regions import regions


class Automation:
    def __init__(self, database, user=None, password=None, host=None, port=5432):
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self.conn = None
        self.cur = None
        self.db_connection()
        table_regions = """
        CREATE TABLE IF NOT EXISTS regions (
            id SERIAL PRIMARY KEY,
            name VARCHAR(200)
        );
        """
        self.cur.execute(table_regions)
        self.cur.execute("SELECT 1 FROM regions LIMIT 1;")
        table_is_full = self.cur.fetchone()

        if not table_is_full:
            for element in regions:
                insert_query = """
                INSERT INTO regions (name)
                VALUES (%s);
                """
                self.cur.execute(insert_query, (element,))
    

    def db_connection(self):
        if self.conn is None:
            try:
                self.conn = psycopg2.connect(
                    host=self.host,
                    port=self.port,
                    database=self.database,
                    user=self.user,
                    password=self.password
                )
                print("Подключено к базе данных.")
                self.cur = self.conn.cursor()
            except psycopg2.Error as e:
                print("Ошибка во время подключения: ", e)
        else:
            print("Уже подлючено к базе данных.")
    

    def insert_gdp(self):
        url = 'https://taldau.stat.gov.kz/ru/Api/GetIndexData/2709379?period=7&dics=67' 
        response = requests.get(url)
        if response.status_code == 200:
            try:
                table_gdp = """
                CREATE TABLE IF NOT EXISTS gdp (
                    id SERIAL PRIMARY KEY,
                    created_at DATE,
                    value BIGINT,
                    updated_at DATE,
                    region_id INT REFERENCES regions(id)
                );
                """

                insert_query = """
                INSERT INTO gdp (created_at, value, updated_at, region_id)
                VALUES (TO_DATE(%s, 'DD.MM.YYYY'), %s, CURRENT_DATE, (SELECT id FROM regions WHERE name = %s));
                """

                self.cur.execute(table_gdp)

                self.cur.execute("SELECT MAX(updated_at) FROM gdp;")
                latest_date = self.cur.fetchone()[0]
                latest_date = latest_date - timedelta(days=1) if latest_date else datetime.strptime("1.1.1970", "%d.%m.%Y").date()


                data = [row for row in response.json()]
                filtered_data = []

                for row in data:
                    unit_data = []
                    for period in row["periods"]:
                        date_object = datetime.strptime(period["date"], "%d.%m.%Y").date()
                        if date_object > latest_date:
                            unit_data = [period["date"], period["value"], row["termNames"][0]]
                            filtered_data.append(unit_data)

                insert_data = [(row[0], row[1], row[2]) for row in filtered_data]

                self.cur.executemany(insert_query, insert_data)
                self.conn.commit()
                
                print("Данные о ВВП загружены.")
            except Exception as e:
                self.conn.rollback()
                print("Произошла ошибка:", str(e))
        else:
            print(f"Ошибка запроса: {response.status_code}")
            print("Новая попытка")
            self.insert_gdp()


    def db_disconnect(self):
        if self.conn is not None:
            self.conn.close()
            self.conn = None
            self.cur = None
            print("Отключено от базы данных.")
        else:
            print("Уже отключено от базы данных.")
    

    def collect_data_years(self):
        self.insert_gdp()


    def collect_data_quarters(self):
        pass


    def collect_data_months(self):
        pass


    def collect_data_weeks(self):
        pass



automation = Automation("statistics", "postgres", "123456")
automation.collect_data_years()
automation.db_disconnect()
