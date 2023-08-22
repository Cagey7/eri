from datetime import datetime
import psycopg2
import requests
from regions import regions
from time import sleep


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


    def get_response(self, url):
        response = requests.get(url)
        if response.status_code == 200:
            return response
        else:
            print(f"Ошибка запроса: {response.status_code}")
            print("Новая попытка")
            sleep(3)
            return self.get_response(url)
    

    def get_latest_updated_date(self, table_name):
        self.cur.execute("SELECT MAX(updated_at) FROM {};".format(table_name))
        latest_date = self.cur.fetchone()[0]
        return latest_date if latest_date else datetime.strptime("1.1.1970", "%d.%m.%Y").date()


    def insert_gdp(self):
        urls = [{"response": self.get_response("https://taldau.stat.gov.kz/ru/Api/GetIndexData/2709379?period=7&dics=67").json(), 
                "period":"Год"}, 
                {"response": self.get_response("https://taldau.stat.gov.kz/ru/Api/GetIndexData/2709379?period=9&dics=67").json(), 
                "period":"Квартал с накоплением"}]
        try:
            table_gdp = """
            CREATE TABLE IF NOT EXISTS gdp (
                id SERIAL PRIMARY KEY,
                region VARCHAR(100),
                created_at DATE,
                value NUMERIC,
                period VARCHAR(100),
                description VARCHAR(200),
                updated_at DATE
            );
            """

            insert_query = """
            INSERT INTO gdp (region, created_at, value, period, description, updated_at)
            VALUES (%s, TO_DATE(%s, 'DD.MM.YYYY'), %s, %s, %s, CURRENT_DATE);
            """

            self.cur.execute(table_gdp)
            latest_date = self.get_latest_updated_date("gdp")

            for url in urls:
                data = [row for row in url["response"]]
                filtered_data = []

                for row in data:
                    unit_data = []
                    for period in row["periods"]:
                        date_object = datetime.strptime(period["date"], "%d.%m.%Y").date()
                        if date_object > latest_date:
                            unit_data = [row["termNames"][0], period["date"], period["value"], url["period"], period["name"]]
                            filtered_data.append(unit_data)

                insert_data = [(row[0], row[1], row[2], row[3], row[4]) for row in filtered_data]
                self.cur.executemany(insert_query, insert_data)
            
            self.conn.commit()
            print("Данные о ВВП загружены.")
        except Exception as e:
            self.conn.rollback()
            print("Произошла ошибка:", str(e))



    def insert_labor_productivity(self):
        urls = [{"response": self.get_response("https://taldau.stat.gov.kz/ru/Api/GetIndexData/4023003?period=7&dics=67,915").json(), 
                 "period":"Год"}, 
                {"response": self.get_response("https://taldau.stat.gov.kz/ru/Api/GetIndexData/4023003?period=9&dics=67,915").json(), 
                 "period":"Квартал с накоплением"}]
        try:
            table_labor_productivity = """
            CREATE TABLE IF NOT EXISTS labor_productivity (
                id SERIAL PRIMARY KEY,
                region VARCHAR(100),
                activity_type VARCHAR(100),
                created_at DATE, 
                value BIGINT,
                period VARCHAR(100),
                description VARCHAR(100),
                updated_at DATE
            );
            """
            insert_query = """
            INSERT INTO labor_productivity (region, activity_type, created_at, value, period, description, updated_at)
            VALUES (%s, %s, TO_DATE(%s, 'DD.MM.YYYY'), %s, %s, %s, CURRENT_DATE);
            """

            self.cur.execute(table_labor_productivity)
            latest_date = self.get_latest_updated_date("labor_productivity")

            for url in urls:
                data = []
                filtered_data = []

                for row in url["response"]:
                    data.append(row)

                for row in data:
                    unit_data = []
                    for period in row["periods"]:
                        date_object = datetime.strptime(period["date"], "%d.%m.%Y").date()
                        if date_object > latest_date:
                            unit_data = [row["termNames"][0], row["termNames"][1], period["date"], period["value"], url["period"], period["name"]]
                            filtered_data.append(unit_data)

                insert_data = [(row[0], row[1], row[2], row[3], row[4], row[5]) for row in filtered_data]
                self.cur.executemany(insert_query, insert_data)

            self.conn.commit()
            print("Данные об производительнсоти труда загружены")
        except Exception as e:
            self.conn.rollback()
            print("Произошла ошибка:", str(e))
    

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
        self.insert_labor_productivity()


    def collect_data_quarters(self):
        pass


    def collect_data_months(self):
        pass


    def collect_data_weeks(self):
        pass


automation = Automation("statistics", "postgres", "123456")
automation.collect_data_years()
automation.db_disconnect()