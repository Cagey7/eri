from datetime import datetime 
import psycopg2
import requests


class Automation:
    def __init__(self, database, user=None, password=None, host=None, port=5432):
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self.conn = None
        self.cur = None
    
    def dbconnection(self):
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
        response = requests.post(url)
        if response.status_code == 200:
            table_gdp_regions = """
            CREATE TABLE IF NOT EXISTS gdp_regions (
                id SERIAL PRIMARY KEY,
                name VARCHAR(200)
            );
            """
            table_gdp = """
            CREATE TABLE IF NOT EXISTS gdp (
                id SERIAL PRIMARY KEY,
                created_at DATE,
                value BIGINT,
                updated_at DATE,
                region_id INT REFERENCES gdp_regions(id)
            );
            """

            self.cur.execute(table_gdp_regions)
            self.cur.execute(table_gdp)

            self.cur.execute("SELECT MAX(updated_at) FROM gdp;")
            latest_date = self.cur.fetchone()[0]
            if latest_date == None:
                latest_date = datetime.strptime("1.1.1970", "%d.%m.%Y").date()


            data = response.json()
            kz_data = []
            filter_data = []
            regions = set()

            for row in data:
                kz_data.append(row)

            for row in kz_data:
                regions.add(row["termNames"][0])
            
            for row in kz_data:
                unit_data = []
                for period in row["periods"]:
                    date_object = datetime.strptime(period["date"], "%d.%m.%Y").date()
                    if date_object > latest_date:
                        unit_data = [period["date"], period["value"], row["termNames"][0]]
                        filter_data.append(unit_data)
            

            self.cur.execute("SELECT 1 FROM gdp_regions LIMIT 1;")
            row_exists = self.cur.fetchone()

            if not row_exists:
                for element in regions:
                    insert_query = """
                    INSERT INTO gdp_regions (name)
                    VALUES (%s);
                    """
                    self.cur.execute(insert_query, (element,))

            for row in filter_data:
                date = row[0]
                value = row[1]
                name = row[2]
                insert_query = """
                INSERT INTO gdp (created_at, value, updated_at, region_id)
                VALUES (TO_DATE(%s, 'DD.MM.YYYY'), %s, CURRENT_DATE, (SELECT id FROM gdp_regions WHERE name = %s));
                """
                self.cur.execute(insert_query, (date, value, name))
        
            self.conn.commit()
            print("Данные о ВВП загружены.")
        else:
            print(f"Ошибка запроса: {response.status_code}")
            print("Новая попытка")
            self.insert_gdp()


    def dbdisconnect(self):
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
automation.dbconnection()
automation.collect_data_years()
automation.dbdisconnect()
