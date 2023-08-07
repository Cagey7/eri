import psycopg2
import requests


class Automation:
    def __init__(self, dbname, user, password):
        self.conn = psycopg2.connect(f"dbname={dbname} user={user} password={password}")
        self.cur = self.conn.cursor()
        self.regions = ('АЛМАТИНСКАЯ ОБЛАСТЬ', 
            'Г.АСТАНА', 
            'ЗАПАДНО-КАЗАХСТАНСКАЯ ОБЛАСТЬ', 
            'ТУРКЕСТАНСКАЯ ОБЛАСТЬ', 
            'АТЫРАУСКАЯ ОБЛАСТЬ', 
            'ПАВЛОДАРСКАЯ ОБЛАСТЬ', 
            'КОСТАНАЙСКАЯ ОБЛАСТЬ', 
            'КЫЗЫЛОРДИНСКАЯ ОБЛАСТЬ', 
            'МАНГИСТАУСКАЯ ОБЛАСТЬ', 
            'ЮЖНО-КАЗАХСТАНСКАЯ ОБЛАСТЬ', 
            'Г.ШЫМКЕНТ', 
            'ОБЛАСТЬ ЖЕТІСУ', 
            'Г.АЛМАТЫ', 
            'ОБЛАСТЬ АБАЙ', 
            'ОБЛАСТЬ ҰЛЫТАУ', 
            'СЕВЕРО-КАЗАХСТАНСКАЯ ОБЛАСТЬ', 
            'АКМОЛИНСКАЯ ОБЛАСТЬ', 
            'КАРАГАНДИНСКАЯ ОБЛАСТЬ', 
            'АКТЮБИНСКАЯ ОБЛАСТЬ', 
            'ВОСТОЧНО-КАЗАХСТАНСКАЯ ОБЛАСТЬ', 
            'ЖАМБЫЛСКАЯ ОБЛАСТЬ')


    def create_tables(self):
        create_table_regions = """
        CREATE TABLE regions (
            id SERIAL PRIMARY KEY,
            name VARCHAR(200)
        );
        """

        create_table_gdp_production_method = """
        CREATE TABLE gdp_production_method (
            id SERIAL PRIMARY KEY,
            created_at DATE,
            value BIGINT,
            updated_at DATE,
            region_id INT REFERENCES regions(id)
        );
        """

        create_table_economic_activity = """
        CREATE TABLE economic_activity_types (
            id SERIAL PRIMARY KEY,
            name VARCHAR(200)
        );
        """

        create_table_labor_productivity = """
        CREATE TABLE labor_productivity (
            id SERIAL PRIMARY KEY,
            created_at DATE,
            value BIGINT,
            updated_at DATE,
            economic_activity_id INT REFERENCES economic_activity_types(id),
            region_id INT REFERENCES regions(id)
        );
        """

        create_table_productional_activity = """
        CREATE TABLE production (
            id SERIAL PRIMARY KEY,
            name VARCHAR(200)
        );
        """   
    
    
        create_table_industrial_production = """
        CREATE TABLE industrial_production (
            id SERIAL PRIMARY KEY,
            created_at DATE,
            percent FLOAT,
            periods_correlation VARCHAR(150),
            updated_at DATE,
            production_id INT REFERENCES production(id),
            region_id INT REFERENCES regions(id)
        );
        """

        create_table_cities = """
        CREATE TABLE cities (
            id SERIAL PRIMARY KEY,
            name VARCHAR(200)
        );
        """
        
        create_table_goods = """
        CREATE TABLE goods (
            id SERIAL PRIMARY KEY,
            name VARCHAR(200)
        );
        """

        create_table_soc_imp_goods_indexes = """
        CREATE TABLE soc_imp_goods_indexes (
            id SERIAL PRIMARY KEY,
            created_at DATE,
            percent FLOAT,
            periods_correlation VARCHAR(150),
            updated_at DATE,
            city_id INT REFERENCES cities(id),
            good_id INT REFERENCES goods(id)
        );
        """

        self.cur.execute(create_table_regions)
        self.cur.execute(create_table_economic_activity)
        self.cur.execute(create_table_labor_productivity)
        self.cur.execute(create_table_gdp_production_method)
        self.cur.execute(create_table_productional_activity)
        self.cur.execute(create_table_industrial_production)
        self.cur.execute(create_table_cities)
        self.cur.execute(create_table_goods)
        self.cur.execute(create_table_soc_imp_goods_indexes)


        for element in self.regions:
            insert_query = """
            INSERT INTO regions (name)
            VALUES (%s);
            """
            self.cur.execute(insert_query, (element,))
        
        self.conn.commit()
    

    def insert_gdp_data(self):
        url = 'https://taldau.stat.gov.kz/ru/Api/GetIndexData/2709379?period=7&dics=67' 
        response = requests.post(url)

        if response.status_code == 200:
            data = response.json()
            kz_data = []
            filter_data = []
            for row in data:
                if row["terms"][0] != 741880:
                    kz_data.append(row)

            for row in kz_data:
                unit_data = []
                for period in row["periods"]:
                    unit_data = [period["date"], period["value"], row["termNames"][0]]
                    filter_data.append(unit_data)
            
        else:
            print(f"Ошибка запроса: {response.status_code}")



        for row in filter_data:
            date = row[0]
            value = row[1]
            name = row[2]

            insert_query = """
            INSERT INTO gdp_production_method (created_at, value, updated_at, region_id)
            VALUES (TO_DATE(%s, 'DD.MM.YYYY'), %s, CURRENT_DATE, (SELECT id FROM regions WHERE name = %s));
            """
            self.cur.execute(insert_query, (date, value, name))
        
        self.conn.commit()
    

    def insert_labour_efficiency(self):
        url = 'https://taldau.stat.gov.kz/ru/Api/GetIndexData/4023003?period=7&dics=67,915' 
        response = requests.post(url) 

        if response.status_code == 200:
            data = response.json()
            activities = set()
            index_data = []
            filter_data = []
            
            for row in data:
                if row["terms"][0] != 741880:
                    index_data.append(row)
                    activities.add(row["termNames"][1])

            

            for row in index_data:
                unit_data = []
                for period in row["periods"]:

                    unit_data = [period["date"], period["value"], row["termNames"][1], row["termNames"][0]]
                    filter_data.append(unit_data)
        else:
            print(f"Ошибка запроса: {response.status_code}")


        for element in activities:
            insert_query = """
            INSERT INTO economic_activity_types (name)
            VALUES (%s);
            """
            self.cur.execute(insert_query, (element,))


        for row in filter_data:
            date = row[0]
            value = row[1]
            activity_name = row[2]
            region_name = row[3]

            insert_query = """
            INSERT INTO labor_productivity (created_at, value, updated_at, economic_activity_id, region_id)
            VALUES (TO_DATE(%s, 'DD.MM.YYYY'), %s, CURRENT_DATE, (SELECT id FROM economic_activity_types WHERE name = %s), (SELECT id FROM regions WHERE name = %s));
            """
            self.cur.execute(insert_query, (date, value, activity_name, region_name))


            self.conn.commit()

    def insert_industrial_production(self):
        url = 'https://taldau.stat.gov.kz/ru/Api/GetIndexData/701625?period=7&dics=68,4303,848' 
        response = requests.post(url) 
        data = response.json()
        index_data = []
        filter_data = []
        production = set()

        if response.status_code == 200:
            for row in data:
                if row["terms"][0] != 741880:
                    index_data.append(row)
                    production.add(row["termNames"][1])

            for row in index_data:
                unit_data = []
                for period in row["periods"]:

                    unit_data = [period["date"], period["value"], row["termNames"][2], row["termNames"][1], row["termNames"][0]]
                    filter_data.append(unit_data)
        else:
            print(f"Ошибка запроса: {response.status_code}")

        for element in production:          
            insert_query = """
            INSERT INTO production (name)
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
            INSERT INTO industrial_production (created_at, percent, periods_correlation, updated_at, production_id, region_id)
            VALUES (TO_DATE(%s, 'DD.MM.YYYY'), %s, %s, CURRENT_DATE, (SELECT id FROM production WHERE name = %s), (SELECT id FROM regions WHERE name = %s));
            """
            self.cur.execute(insert_query, (date, value, period, production_name, region_name))
        
        self.conn.commit()
            


    def insert_soc_imp_goods_indexes(self):
        url = 'https://taldau.stat.gov.kz/ru/Api/GetIndexData/18808243?period=2&dics=67,4305,848' 
        response = requests.post(url)
        data = response.json()
        cities = set()
        goods = set()
        index_data = []
        filter_data = []

        if response.status_code == 200:

            
            for row in data:
                if row["terms"][0] != 741880:
                    index_data.append(row)
                    cities.add(row["termNames"][0])
                    goods.add(row["termNames"][1])

            for row in index_data:
                unit_data = []
                for period in row["periods"]:
                    if period["value"] != "x":
                        value = float(period["value"])
                    else:
                        value = None
                    unit_data = [period["date"], value, row["termNames"][2], row["termNames"][0], row["termNames"][1]]
                    filter_data.append(unit_data)
        else:
            print(f"Ошибка запроса: {response.status_code}")
        
        
        for element in goods:
            insert_query = """
            INSERT INTO goods (name)
            VALUES (%s);
            """
            self.cur.execute(insert_query, (element,))
            
        for element in cities:
            insert_query = """
            INSERT INTO cities (name)
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
            INSERT INTO soc_imp_goods_indexes (created_at, percent, periods_correlation, updated_at, city_id, good_id)
            VALUES (TO_DATE(%s, 'DD.MM.YYYY'), %s, %s, CURRENT_DATE, (SELECT id FROM cities WHERE name = %s), (SELECT id FROM goods WHERE name = %s));
            """
            self.cur.execute(insert_query, (date, value, period_corr, city, good))


        self.conn.commit()


    def collect_data(self):
        self.create_tables()
        self.insert_gdp_data()
        self.insert_labour_efficiency()
        self.insert_industrial_production()
        self.insert_soc_imp_goods_indexes()
        self.disconect_db()


    def connect_db(self, dbname, user, password):
        self.conn = psycopg2.connect(f"dbname={dbname} user={user} password={password}")
        self.cur = self.conn.cursor()


    def disconect_db(self):
        self.cur.close()
        self.conn.close()


automation = Automation("statistics", "postgres", "123456")
automation.collect_data()