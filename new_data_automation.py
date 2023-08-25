from datetime import datetime
import time
import psycopg2
import requests
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
        start_time = time.time()
        try:
            urls = [{"response": self.get_response("https://taldau.stat.gov.kz/ru/Api/GetIndexData/2709379?period=7&dics=67").json(), 
                    "period":"Год"}, 
                    {"response": self.get_response("https://taldau.stat.gov.kz/ru/Api/GetIndexData/2709379?period=9&dics=67").json(), 
                    "period":"Квартал с накоплением"}]
        except Exception as e:
            print("Произошла ошибка:", str(e))
        
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
                insert_data = []

                for row in url["response"]:
                    unit_data = []
                    for period in row["periods"]:
                        if period["value"] == "x":
                            period["value"] = -1
                        date_object = datetime.strptime(period["date"], "%d.%m.%Y").date()
                        if date_object > latest_date:
                            unit_data = (row["termNames"][0], period["date"], period["value"], url["period"], period["name"])
                            insert_data.append(unit_data)

                self.cur.executemany(insert_query, insert_data)
            
            self.conn.commit()
            end_time = time.time()
            print(f"Данные о ВВП загружены за {end_time - start_time:.2f} секунд.")
        except Exception as e:
            self.conn.rollback()
            print("Произошла ошибка:", str(e))


    def insert_labor_productivity(self):
        start_time = time.time()
        try:
            urls = [{"response": self.get_response("https://taldau.stat.gov.kz/ru/Api/GetIndexData/4023003?period=7&dics=67,915").json(), 
                    "period":"Год"}, 
                    {"response": self.get_response("https://taldau.stat.gov.kz/ru/Api/GetIndexData/4023003?period=9&dics=67,915").json(), 
                    "period":"Квартал с накоплением"}]
        except Exception as e:
            print("Произошла ошибка:", str(e))
        
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
                insert_data = []

                for row in url["response"]:
                    unit_data = []
                    for period in row["periods"]:
                        if period["value"] == "x":
                            period["value"] = -1
                        date_object = datetime.strptime(period["date"], "%d.%m.%Y").date()
                        if date_object > latest_date:
                            unit_data = (row["termNames"][0], row["termNames"][1], period["date"], period["value"], url["period"], period["name"])
                            insert_data.append(unit_data)

                self.cur.executemany(insert_query, insert_data)

            self.conn.commit()
            end_time = time.time()
            print(f"Данные об производительнсоти труда загружены за {end_time - start_time:.2f} секунд.")
        except Exception as e:
            self.conn.rollback()
            print("Произошла ошибка:", str(e))
    
    
    def insert_consumer_price_index(self):
        start_time = time.time()
        try:
            response = self.get_response("https://taldau.stat.gov.kz/ru/Api/GetIndexData/703076?period=4&dics=67,848,2753&dateIds=982").json()
        except Exception as e:
            print("Произошла ошибка:", str(e))

        try:
            table_consumer_price_index = """
            CREATE TABLE IF NOT EXISTS consumer_price_index (
                id SERIAL PRIMARY KEY,
                region VARCHAR(100),
                activity_type VARCHAR(200),
                created_at DATE, 
                value FLOAT,
                description VARCHAR(200),
                periods_correlation VARCHAR(200),
                updated_at DATE
            );
            """
            insert_query = """
            INSERT INTO consumer_price_index (region, activity_type, created_at, value, description, periods_correlation, updated_at)
            VALUES (%s, %s, TO_DATE(%s, 'DD.MM.YYYY'), %s, %s, %s, CURRENT_DATE);
            """

            self.cur.execute(table_consumer_price_index)
            latest_date = self.get_latest_updated_date("consumer_price_index")

            insert_data = []

            for row in response:
                unit_data = []
                for period in row["periods"]:
                    if period["value"] == "x":
                        period["value"] = -1
                    date_object = datetime.strptime(period["date"], "%d.%m.%Y").date()
                    if date_object > latest_date:
                        unit_data = (row["termNames"][0], row["termNames"][2], period["date"], period["value"], period["name"], row["termNames"][1])
                        insert_data.append(unit_data)
            self.cur.executemany(insert_query, insert_data)

            self.conn.commit()
            end_time = time.time()
            print(f"Данные об индексе потребительских цен загружены за {end_time - start_time:.2f} секунд.")
        except Exception as e:
            self.conn.rollback()
            print("Произошла ошибка:", str(e))


    def insert_producer_price_index(self):
        start_time = time.time()
        try:
            response = self.get_response("https://taldau.stat.gov.kz/ru/Api/GetIndexData/703039?period=4&dics=68,848,2513,2854,3068&dateIds=982").json()
        except Exception as e:
            print("Произошла ошибка:", str(e))
        
        try:
            table_producer_price_index = """
            CREATE TABLE IF NOT EXISTS producer_price_index (
                id SERIAL PRIMARY KEY,
                region VARCHAR(100),
                activity_type VARCHAR(200),
                created_at DATE, 
                value FLOAT,
                description VARCHAR(200),
                periods_correlation VARCHAR(200),
                countries VARCHAR(200),
                industrial_products_list VARCHAR(300),
                updated_at DATE
            );
            """
            insert_query = """
            INSERT INTO producer_price_index (region, activity_type, created_at, value, description, periods_correlation, countries, industrial_products_list, updated_at)
            VALUES (%s, %s, TO_DATE(%s, 'DD.MM.YYYY'), %s, %s, %s, %s, %s, CURRENT_DATE);
            """

            self.cur.execute(table_producer_price_index)
            latest_date = self.get_latest_updated_date("producer_price_index")
            
            insert_data = []

            for row in response:
                unit_data = []
                for period in row["periods"]:
                    if period["value"] == "x":
                        period["value"] = -1
                    date_object = datetime.strptime(period["date"], "%d.%m.%Y").date()
                    if date_object > latest_date:
                        unit_data = (row["termNames"][0], row["termNames"][3], period["date"], period["value"], period["name"], row["termNames"][1], row["termNames"][2], row["termNames"][4])
                        insert_data.append(unit_data)

            self.cur.executemany(insert_query, insert_data)
                
            self.conn.commit()
            end_time = time.time()
            print(f"Данные об индексе цен производителей загружены за {end_time - start_time:.2f} секунд.")
        except Exception as e:
            self.conn.rollback()
            print("Произошла ошибка:", str(e))


    def insert_soc_imp_goods_price_index(self):
        start_time = time.time()
        try:
            response = self.get_response("https://taldau.stat.gov.kz/ru/Api/GetIndexData/18808243?period=2&dics=67,4305,848").json()
        except Exception as e:
            print("Произошла ошибка:", str(e))
        
        try:
            table_soc_imp_goods_price_index = """
            CREATE TABLE IF NOT EXISTS soc_imp_goods_price_index (
                id SERIAL PRIMARY KEY,
                region VARCHAR(100),
                social_important_goods VARCHAR(200),
                created_at DATE, 
                value FLOAT,
                description VARCHAR(200),
                periods_correlation VARCHAR(200),
                updated_at DATE
            );
            """
            insert_query = """
            INSERT INTO soc_imp_goods_price_index (region, social_important_goods, created_at, value, description, periods_correlation, updated_at)
            VALUES (%s, %s, TO_DATE(%s, 'DD.MM.YYYY'), %s, %s, %s, CURRENT_DATE);
            """

            self.cur.execute(table_soc_imp_goods_price_index)
            latest_date = self.get_latest_updated_date("soc_imp_goods_price_index")

            insert_data = []

            for row in response:
                unit_data = []
                for period in row["periods"]:
                    if period["value"] == "x":
                        period["value"] = -1
                    date_object = datetime.strptime(period["date"], "%d.%m.%Y").date()
                    if date_object > latest_date:
                        unit_data = (row["termNames"][0], row["termNames"][1], period["date"], period["value"], period["name"], row["termNames"][2])
                        insert_data.append(unit_data)

            self.cur.executemany(insert_query, insert_data)
                
            self.conn.commit()
            end_time = time.time()
            print(f"Данные об индексе цен на социально-значимые продовольственные товары загружены за {end_time - start_time:.2f} секунд.")
        except Exception as e:
            self.conn.rollback()
            print("Произошла ошибка:", str(e))
    

    def insert_avmon_nom_wages_year(self):
        start_time = time.time()
        try:
            response = self.get_response("https://taldau.stat.gov.kz/ru/Api/GetIndexData/702972?period=7&dics=68,859,776,2813,576").json()
        except Exception as e:
            print("Произошла ошибка:", str(e))
        
            
        try:    
            table_avmon_nom_wages_year = """
            CREATE TABLE IF NOT EXISTS avmon_nom_wages_year (
                id SERIAL PRIMARY KEY,
                region VARCHAR(100),
                activity_type VARCHAR(300),
                created_at DATE, 
                value FLOAT,
                description VARCHAR(200),
                terrain_type VARCHAR(100),
                enterprise_dimension VARCHAR(100),
                gender VARCHAR(100),
                updated_at DATE
            );
            """
            insert_query = """
            INSERT INTO avmon_nom_wages_year (region, activity_type, created_at, value, description, terrain_type, enterprise_dimension, gender, updated_at)
            VALUES (%s, %s, TO_DATE(%s, 'DD.MM.YYYY'), %s, %s, %s, %s, %s, CURRENT_DATE);
            """

            self.cur.execute(table_avmon_nom_wages_year)
            latest_date = self.get_latest_updated_date("avmon_nom_wages_year")

            insert_data = []

            for row in response:
                unit_data = []
                for period in row["periods"]:
                    if period["value"] == "x":
                        period["value"] = -1
                    date_object = datetime.strptime(period["date"], "%d.%m.%Y").date()
                    if date_object > latest_date:
                        unit_data = (row["termNames"][0], row["termNames"][1], period["date"], period["value"], period["name"], row["termNames"][2], row["termNames"][3], row["termNames"][4])
                        insert_data.append(unit_data)

            self.cur.executemany(insert_query, insert_data)
                
            self.conn.commit()
            end_time = time.time()
            print(f"Данные об cреднемесячной номинальной заработной плате по ВЭД за год загружены за {end_time - start_time:.2f} секунд.")
        except Exception as e:
            self.conn.rollback()
            print("Произошла ошибка:", str(e))


    def insert_avmon_nom_wages_quarter(self):
        start_time = time.time()
        try:
            response = self.get_response("https://taldau.stat.gov.kz/ru/Api/GetIndexData/702972?period=5&dics=68,859,681").json()
        except Exception as e:
            print("Произошла ошибка:", str(e))
        
        try:
            table_avmon_nom_wages_quarter = """
            CREATE TABLE IF NOT EXISTS avmon_nom_wages_quarter (
                id SERIAL PRIMARY KEY,
                region VARCHAR(100),
                activity_type VARCHAR(300),
                created_at DATE, 
                value FLOAT,
                description VARCHAR(200),
                economic_sectors VARCHAR(100),
                updated_at DATE
            );
            """
            insert_query = """
            INSERT INTO avmon_nom_wages_quarter (region, activity_type, created_at, value, description, economic_sectors, updated_at)
            VALUES (%s, %s, TO_DATE(%s, 'DD.MM.YYYY'), %s, %s, %s, CURRENT_DATE);
            """

            self.cur.execute(table_avmon_nom_wages_quarter)
            latest_date = self.get_latest_updated_date("avmon_nom_wages_quarter")
            
            insert_data = []

            for row in response:
                unit_data = []
                for period in row["periods"]:
                    if period["value"] == "x":
                        period["value"] = -1
                    date_object = datetime.strptime(period["date"], "%d.%m.%Y").date()
                    if date_object > latest_date:
                        unit_data = (row["termNames"][0], row["termNames"][1], period["date"], period["value"], period["name"], row["termNames"][2])
                        insert_data.append(unit_data)

            self.cur.executemany(insert_query, insert_data)
                
            self.conn.commit()
            end_time = time.time()
            print(f"Данные об cреднемесячной номинальной заработной плате по ВЭД за квартал загружены за {end_time - start_time:.2f} секунд.")
        except Exception as e:
            self.conn.rollback()
            print("Произошла ошибка:", str(e))


    def insert_nom_wages_index_year(self):
        start_time = time.time()
        try:
            response = self.get_response("https://taldau.stat.gov.kz/ru/Api/GetIndexData/702974?period=7&dics=68,859,2813,576,848").json()
        except Exception as e:
            print("Произошла ошибка:", str(e))
        
        try:
            table_nom_wages_index_year = """
            CREATE TABLE IF NOT EXISTS nom_wages_index_year (
                id SERIAL PRIMARY KEY,
                region VARCHAR(100),
                activity_type VARCHAR(300),
                created_at DATE, 
                value FLOAT,
                description VARCHAR(200),
                enterprise_dimension VARCHAR(100),
                gender VARCHAR(100),
                periods_correlation VARCHAR(200),
                updated_at DATE
            );
            """
            insert_query = """
            INSERT INTO nom_wages_index_year (region, activity_type, created_at, value, description, enterprise_dimension, gender, periods_correlation, updated_at)
            VALUES (%s, %s, TO_DATE(%s, 'DD.MM.YYYY'), %s, %s, %s, %s, %s, CURRENT_DATE);
            """

            self.cur.execute(table_nom_wages_index_year)
            latest_date = self.get_latest_updated_date("nom_wages_index_year")

            insert_data = []

            for row in response:
                unit_data = []
                for period in row["periods"]:
                    if period["value"] == "x":
                        period["value"] = -1
                    date_object = datetime.strptime(period["date"], "%d.%m.%Y").date()
                    if date_object > latest_date:
                        unit_data = (row["termNames"][0], row["termNames"][1], period["date"], period["value"], period["name"], row["termNames"][2], row["termNames"][3], row["termNames"][4])
                        insert_data.append(unit_data)

            self.cur.executemany(insert_query, insert_data)
                
            self.conn.commit()
            end_time = time.time()
            print(f"Данные об индексе номинальной заработной платы по ВЭД за год загружены за {end_time - start_time:.2f} секунд.")
        except Exception as e:
            self.conn.rollback()
            print("Произошла ошибка:", str(e))


    def insert_nom_wages_index_quarter(self):
        start_time = time.time()
        try:
            response = self.get_response("https://taldau.stat.gov.kz/ru/Api/GetIndexData/702974?period=5&dics=68,859,2813,848").json()
        except Exception as e:
            print("Произошла ошибка:", str(e))
        
        try:    
            table_nom_wages_index_quarter = """
            CREATE TABLE IF NOT EXISTS nom_wages_index_quarter (
                id SERIAL PRIMARY KEY,
                region VARCHAR(100),
                activity_type VARCHAR(300),
                created_at DATE, 
                value FLOAT,
                description VARCHAR(200),
                enterprise_dimension VARCHAR(100),
                periods_correlation VARCHAR(200),
                updated_at DATE
            );
            """
            insert_query = """
            INSERT INTO nom_wages_index_quarter (region, activity_type, created_at, value, description, enterprise_dimension, periods_correlation, updated_at)
            VALUES (%s, %s, TO_DATE(%s, 'DD.MM.YYYY'), %s, %s, %s, %s, CURRENT_DATE);
            """

            self.cur.execute(table_nom_wages_index_quarter)
            latest_date = self.get_latest_updated_date("nom_wages_index_quarter")
            
            insert_data = []

            for row in response:
                unit_data = []
                for period in row["periods"]:
                    if period["value"] == "x":
                        period["value"] = -1
                    date_object = datetime.strptime(period["date"], "%d.%m.%Y").date()
                    if date_object > latest_date:
                        unit_data = (row["termNames"][0], row["termNames"][1], period["date"], period["value"], period["name"], row["termNames"][2], row["termNames"][3])
                        insert_data.append(unit_data)

            self.cur.executemany(insert_query, insert_data)
                
            self.conn.commit()
            end_time = time.time()
            print(f"Данные об индексе номинальной заработной платы по ВЭД за квартал загружены за {end_time - start_time:.2f} секунд.")
        except Exception as e:
            self.conn.rollback()
            print("Произошла ошибка:", str(e))


    def insert_real_wages_index_year(self):
        start_time = time.time()
        try:
            response = self.get_response("https://taldau.stat.gov.kz/ru/Api/GetIndexData/702976?period=7&dics=68,859,2813,576,848").json()
        except Exception as e:
            print("Произошла ошибка:", str(e))
        
        try:
            table_real_wages_index_year = """
            CREATE TABLE IF NOT EXISTS real_wages_index_year (
                id SERIAL PRIMARY KEY,
                region VARCHAR(100),
                activity_type VARCHAR(300),
                created_at DATE, 
                value FLOAT,
                description VARCHAR(200),
                enterprise_dimension VARCHAR(100),
                gender VARCHAR(100),
                periods_correlation VARCHAR(200),
                updated_at DATE
            );
            """
            insert_query = """
            INSERT INTO real_wages_index_year (region, activity_type, created_at, value, description, enterprise_dimension, gender, periods_correlation, updated_at)
            VALUES (%s, %s, TO_DATE(%s, 'DD.MM.YYYY'), %s, %s, %s, %s, %s, CURRENT_DATE);
            """

            self.cur.execute(table_real_wages_index_year)
            latest_date = self.get_latest_updated_date("real_wages_index_year")
            
            insert_data = []

            for row in response:
                unit_data = []
                for period in row["periods"]:
                    if period["value"] == "x":
                        period["value"] = -1
                    date_object = datetime.strptime(period["date"], "%d.%m.%Y").date()
                    if date_object > latest_date:
                        unit_data = (row["termNames"][0], row["termNames"][1], period["date"], period["value"], period["name"], row["termNames"][2], row["termNames"][3], row["termNames"][4])
                        insert_data.append(unit_data)

            self.cur.executemany(insert_query, insert_data)
                
            self.conn.commit()
            end_time = time.time()
            print(f"Данные об индексе реальной заработной платы по ВЭД за год загружены за {end_time - start_time:.2f} секунд.")
        except Exception as e:
            self.conn.rollback()
            print("Произошла ошибка:", str(e))


    def insert_real_wages_index_quarter(self):
        start_time = time.time()
        try:
            response = self.get_response("https://taldau.stat.gov.kz/ru/Api/GetIndexData/702976?period=5&dics=68,859,2813,848").json()
        except Exception as e:
            print("Произошла ошибка:", str(e))
        
        try:
            table_real_wages_index_quarter = """
            CREATE TABLE IF NOT EXISTS real_wages_index_quarter (
                id SERIAL PRIMARY KEY,
                region VARCHAR(100),
                activity_type VARCHAR(300),
                created_at DATE, 
                value FLOAT,
                description VARCHAR(200),
                enterprise_dimension VARCHAR(100),
                periods_correlation VARCHAR(200),
                updated_at DATE
            );
            """
            insert_query = """
            INSERT INTO real_wages_index_quarter (region, activity_type, created_at, value, description, enterprise_dimension, periods_correlation, updated_at)
            VALUES (%s, %s, TO_DATE(%s, 'DD.MM.YYYY'), %s, %s, %s, %s, CURRENT_DATE);
            """

            self.cur.execute(table_real_wages_index_quarter)
            latest_date = self.get_latest_updated_date("real_wages_index_quarter")

            insert_data = []

            for row in response:
                unit_data = []
                for period in row["periods"]:
                    if period["value"] == "x":
                        period["value"] = -1
                    date_object = datetime.strptime(period["date"], "%d.%m.%Y").date()
                    if date_object > latest_date:
                        unit_data = (row["termNames"][0], row["termNames"][1], period["date"], period["value"], period["name"], row["termNames"][2], row["termNames"][3])
                        insert_data.append(unit_data)

            self.cur.executemany(insert_query, insert_data)
                
            self.conn.commit()
            end_time = time.time()
            print(f"Данные об индексе реальной заработной платы по ВЭД за квартал загружены за {end_time - start_time:.2f} секунд.")
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
        self.insert_consumer_price_index()
        self.insert_producer_price_index()
        self.insert_soc_imp_goods_price_index()
        self.insert_avmon_nom_wages_year()
        self.insert_avmon_nom_wages_quarter()
        self.insert_nom_wages_index_year()
        self.insert_nom_wages_index_quarter()
        self.insert_real_wages_index_year()
        self.insert_real_wages_index_quarter()


    def collect_data_quarters(self):
        pass


    def collect_data_months(self):
        pass


    def collect_data_weeks(self):
        pass


automation = Automation("statistics", "postgres", "123456")
automation.collect_data_years()
automation.db_disconnect()
