from datetime import datetime 
import psycopg2
import requests


class Automation:
    def __init__(self, dbname, user, password):
        self.conn = psycopg2.connect(f"dbname={dbname} user={user} password={password}")
        self.cur = self.conn.cursor()
    





    def insert_labor_productivity_data(self):
        url = 'https://taldau.stat.gov.kz/ru/Api/GetIndexData/4023003?period=9&dics=67,915' 
        response = requests.post(url) 


        if response.status_code == 200:
            table_labor_productivity_regions = """
            CREATE TABLE labor_productivity_regions (
                id SERIAL PRIMARY KEY,
                name VARCHAR(200)
            );
            """

            table_economic_activity = """
            CREATE TABLE labor_productivity_activity_types (
                id SERIAL PRIMARY KEY,
                name VARCHAR(200)
            );
            """

            table_labor_productivity = """
            CREATE TABLE labor_productivity (
                id SERIAL PRIMARY KEY,
                created_at DATE,
                description VARCHAR(150),
                value BIGINT,
                updated_at DATE,
                economic_activity_id INT REFERENCES labor_productivity_activity_types(id),
                region_id INT REFERENCES labor_productivity_regions(id)
            );
            """
            self.cur.execute(table_labor_productivity_regions)
            self.cur.execute(table_economic_activity)
            self.cur.execute(table_labor_productivity)

            data = response.json()
            regions = set()
            activities = set()
            kz_data = []
            filter_data = []
            
            for row in data:
                kz_data.append(row)
                regions.add(row["termNames"][0])
                activities.add(row["termNames"][1])

            for row in kz_data:
                unit_data = []
                for period in row["periods"]:
                    unit_data = [period["date"], period["value"], period["name"], row["termNames"][1], row["termNames"][0]]
                    filter_data.append(unit_data)
            
            for element in regions:          
                insert_query = """
                INSERT INTO labor_productivity_regions (name)
                VALUES (%s);
                """
                self.cur.execute(insert_query, (element,))
            
            for element in activities:
                insert_query = """
                INSERT INTO labor_productivity_activity_types (name)
                VALUES (%s);
                """
                self.cur.execute(insert_query, (element,))

            for row in filter_data:
                date = row[0]
                value = row[1]
                description = row[2]
                activity_name = row[3]
                region_name = row[4]
                insert_query = """
                INSERT INTO labor_productivity (created_at, value, description, updated_at, economic_activity_id, region_id)
                VALUES (TO_DATE(%s, 'DD.MM.YYYY'), %s, %s, CURRENT_DATE, (SELECT id FROM labor_productivity_activity_types WHERE name = %s), 
                (SELECT id FROM labor_productivity_regions WHERE name = %s));
                """
                self.cur.execute(insert_query, (date, value, description, activity_name, region_name))

            self.conn.commit()
            print("Данный о производительности труда загружены.")
        else:
            print(f"Ошибка запроса: {response.status_code}")
            print("Новая попытка")
            self.insert_labor_productivity_data()

    def insert_volume_index_industrial_data(self):
        url = 'https://taldau.stat.gov.kz/ru/Api/GetIndexData/701625?period=7&dics=68,4303,848' 
        response = requests.post(url) 
        if response.status_code == 200:
            table_volume_index_industrial_regions = """
            CREATE TABLE volume_index_industrial_regions (
                id SERIAL PRIMARY KEY,
                name VARCHAR(200)
            );
            """
            table_volume_index_industrial_productions = """
            CREATE TABLE volume_index_industrial_productions (
                id SERIAL PRIMARY KEY,
                name VARCHAR(200)
            );
            """   

            table_volume_index_industrial = """
            CREATE TABLE volume_index_industrial (
                id SERIAL PRIMARY KEY,
                created_at DATE,
                percent FLOAT,
                periods_correlation VARCHAR(150),
                updated_at DATE,
                production_id INT REFERENCES volume_index_industrial_productions(id),
                region_id INT REFERENCES volume_index_industrial_regions(id)
            );
            """
            self.cur.execute(table_volume_index_industrial_regions)
            self.cur.execute(table_volume_index_industrial_productions)
            self.cur.execute(table_volume_index_industrial)

            data = response.json()
            kz_data = []
            filter_data = []
            regions = set()
            productions = set()
            for row in data:
                kz_data.append(row)
                regions.add(row["termNames"][0])
                productions.add(row["termNames"][1])

            for row in kz_data:
                unit_data = []
                for period in row["periods"]:
                    unit_data = [period["date"], period["value"], row["termNames"][2], row["termNames"][1], row["termNames"][0]]
                    filter_data.append(unit_data)
            
            for element in regions:          
                insert_query = """
                INSERT INTO volume_index_industrial_regions (name)
                VALUES (%s);
                """
                self.cur.execute(insert_query, (element,))

            for element in productions:          
                insert_query = """
                INSERT INTO volume_index_industrial_productions (name)
                VALUES (%s);
                """
                self.cur.execute(insert_query, (element,))

            for row in filter_data:
                date = row[0]
                value = row[1]
                period = row[2]
                production_name = row[3]
                region_name = row[4]
                insert_query = """
                INSERT INTO volume_index_industrial (created_at, percent, periods_correlation, updated_at, production_id, region_id)
                VALUES (TO_DATE(%s, 'DD.MM.YYYY'), %s, %s, CURRENT_DATE, (SELECT id FROM volume_index_industrial_productions WHERE name = %s), 
                (SELECT id FROM volume_index_industrial_regions WHERE name = %s));
                """
                self.cur.execute(insert_query, (date, value, period, production_name, region_name))
            
            self.conn.commit()
            print("Данные о индексе промышленного производства по промышленной продукции загружены.")


        else:
            print(f"Ошибка запроса: {response.status_code}")
            print("Новая попытка")
            self.insert_volume_index_industrial_data()


    def insert_pindex_simportant_consumer_goods_data(self):
        url = 'https://taldau.stat.gov.kz/ru/Api/GetIndexData/18808243?period=2&dics=67,4305,848' 
        response = requests.post(url)
        if response.status_code == 200:
            table_pindex_simportant_consumer_goods_regions = """
            CREATE TABLE IF NOT EXISTS pindex_simportant_consumer_goods_regions (
                id SERIAL PRIMARY KEY,
                name VARCHAR(200)
            );
            """
            table_pindex_simportant_consumer_goods_goods = """
            CREATE TABLE IF NOT EXISTS pindex_simportant_consumer_goods_goods (
                id SERIAL PRIMARY KEY,
                name VARCHAR(200)
            );
            """
            table_pindex_simportant_consumer_goods = """
            CREATE TABLE IF NOT EXISTS pindex_simportant_consumer_goods (
                id SERIAL PRIMARY KEY,
                created_at DATE,
                percent FLOAT,
                periods_correlation VARCHAR(150),
                updated_at DATE,
                region_id INT REFERENCES pindex_simportant_consumer_goods_regions(id),
                goods_id INT REFERENCES pindex_simportant_consumer_goods_goods(id)
            );
            """
            self.cur.execute(table_pindex_simportant_consumer_goods_regions)
            self.cur.execute(table_pindex_simportant_consumer_goods_goods)
            self.cur.execute(table_pindex_simportant_consumer_goods)
            

            get_max_date = "SELECT MAX(updated_at) FROM pindex_simportant_consumer_goods;"
            self.cur.execute(get_max_date)
            latest_date = self.cur.fetchone()[0]
            if latest_date == None:
                latest_date = datetime.strptime("1.1.1970", "%d.%m.%Y").date()
            

            data = response.json()
            regions = set()
            goods = set()
            kz_data = []
            filter_data = []
            for row in data:
                kz_data.append(row)
                regions.add(row["termNames"][0])
                goods.add(row["termNames"][1])

            for row in kz_data:
                unit_data = []
                for period in row["periods"]:
                    if period["value"] != "x":
                        value = float(period["value"])
                    else:
                        value = None
                    date_object = datetime.strptime(period["date"], "%d.%m.%Y").date()
                    if date_object > latest_date:
                        unit_data = [period["date"], value, row["termNames"][2], row["termNames"][0], row["termNames"][1]]
                        filter_data.append(unit_data)

            self.cur.execute("SELECT 1 FROM pindex_simportant_consumer_goods_goods LIMIT 1;")
            row_exists_goods = self.cur.fetchone()

            if not row_exists_goods:
                for element in goods:
                    insert_query = """
                    INSERT INTO pindex_simportant_consumer_goods_goods (name)
                    VALUES (%s);
                    """
                    self.cur.execute(insert_query, (element,))
            
            self.cur.execute("SELECT 1 FROM pindex_simportant_consumer_goods_regions LIMIT 1;")
            row_exists_regions = self.cur.fetchone()

            if not row_exists_regions:
                for element in regions:
                    insert_query = """
                    INSERT INTO pindex_simportant_consumer_goods_regions (name)
                    VALUES (%s);
                    """
                    self.cur.execute(insert_query, (element,))


            for row in filter_data:
                date = row[0]
                value = row[1]
                period_corr = row[2]
                city = row[3]
                good = row[4]
                insert_query = """
                INSERT INTO pindex_simportant_consumer_goods (created_at, percent, periods_correlation, updated_at, region_id, goods_id)
                VALUES (TO_DATE(%s, 'DD.MM.YYYY'), %s, %s, CURRENT_DATE, (SELECT id FROM pindex_simportant_consumer_goods_regions WHERE name = %s),
                (SELECT id FROM pindex_simportant_consumer_goods_goods WHERE name = %s));
                """
                self.cur.execute(insert_query, (date, value, period_corr, city, good))

            self.conn.commit()
            print("Данные об индексе цен на социально-значимые потребительские товары загружены")
        else:
            print(f"Ошибка запроса: {response.status_code}")
            print("Новая попытка")
            self.insert_pindex_simportant_consumer_goods_data()
    
        
    def insert_physical_volume_index(self):
        pass
        
    def collect_data(self):
        print("Идет загрузка...")
        self.insert_gdp_data()
        #self.insert_labor_productivity_data()
        #self.insert_volume_index_industrial_data()
        #self.insert_pindex_simportant_consumer_goods_data()
        self.disconect_db()
        print("Загрузка завершена.")


    def connect_db(self, dbname, user, password):
        self.conn = psycopg2.connect(f"dbname={dbname} user={user} password={password}")
        self.cur = self.conn.cursor()


    def disconect_db(self):
        self.cur.close()
        self.conn.close()


automation = Automation("statistics", "postgres", "123456")
automation.collect_data()
