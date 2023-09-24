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
            sleep(3)
            return self.get_response(url)
    

    def get_latest_updated_date(self, table_name):
        self.cur.execute("SELECT MAX(updated_at) FROM {};".format(table_name))
        latest_date = self.cur.fetchone()[0]
        return latest_date if latest_date else datetime.strptime("1.1.1970", "%d.%m.%Y").date()


    def insert_data(self, table_name, index_name, index_period, url, *fields):
        start_time = time.time()
        try:
            response = self.get_response(url).json()
        except Exception as e:
            print("Произошла ошибка:", str(e))
            print("Возникла ошибка при получении json")

        try:
            full_table_name = f"{table_name}_{index_period}"
            values_for_table = ""
            field_names = ""
            insert_values = ""
            for field in fields:
                field_names += f"{field},"

                if field == "created_at":
                    insert_values += "TO_DATE(%s, 'DD.MM.YYYY'),"
                else:
                    insert_values += "%s,"
                
                if field == "created_at":
                    values_for_table += f"{field} DATE,"
                elif field == "value":
                    values_for_table += f"{field} NUMERIC,"
                else:
                    values_for_table += f"{field} VARCHAR,"
            

            table = f"""
            CREATE TABLE IF NOT EXISTS {full_table_name} (
                id SERIAL PRIMARY KEY,
                {values_for_table}
                updated_at DATE
            );
            """
            self.cur.execute(table)
            insert_query = f"""
            INSERT INTO {full_table_name} ({field_names} updated_at) VALUES ({insert_values} CURRENT_DATE);
            """

            latest_date = self.get_latest_updated_date(full_table_name)

            insert_data = []

            for row in response:
                unit_data = []
                for period in row["periods"]:
                    if period["value"] == "x":
                        period["value"] = -1
                    date_object = datetime.strptime(period["date"], "%d.%m.%Y").date()
                    if date_object > latest_date:
                        unit_data = tuple(row["termNames"]) + (period["date"], period["value"], period["name"])
                        insert_data.append(unit_data)
            self.cur.executemany(insert_query, insert_data)
            
            self.conn.commit()
            end_time = time.time()
            print(f"{index_name} загружен за {index_period} за {end_time - start_time:.2f} секунд.")
        except Exception as e:
            self.conn.rollback()
            print("Произошла ошибка:", str(e))
            print("Возникла ошибка при загрузке данных")


    def create_index(table_name, *fields):
        pass

    def db_disconnect(self):
        if self.conn is not None:
            self.conn.close()
            self.conn = None
            self.cur = None
            print("Отключено от базы данных.")
        else:
            print("Уже отключено от базы данных.")


    def collect_data_years(self):
        self.insert_data("labor_productivity", "Производительность труда", "year", 
                         "https://taldau.stat.gov.kz/ru/Api/GetIndexData/4023003?period=7&dics=67,915", 
                         "region", "activity_type", "created_at", "value", "description")
        # self.insert_data("labor_productivity", "Производительность труда", "quarter_accum", 
        #                  "https://taldau.stat.gov.kz/ru/Api/GetIndexData/4023003?period=9&dics=67,915", 
        #                  "region", "activity_type", "created_at", "value", "description")
        # self.insert_data("gdp", "ВВП", "year", 
        #                  "https://taldau.stat.gov.kz/ru/Api/GetIndexData/2709379?period=7&dics=67", 
        #                  "region", "created_at", "value", "description")
        # self.insert_data("gdp", "ВВП", "quarter_accum", 
        #                  "https://taldau.stat.gov.kz/ru/Api/GetIndexData/2709379?period=9&dics=67", 
        #                  "region", "created_at", "value", "description")
        # self.insert_data("consumer_price_index", "Индекс потребительских цен", "month",
        #                  "https://taldau.stat.gov.kz/ru/Api/GetIndexData/703076?period=4&dics=67,848,2753",
        #                  "region", "periods_correlation", "activity_type", "created_at", "value", "description")
        # self.insert_data("producer_price_index", "Индекс цен производителей", "month",
        #                  "https://taldau.stat.gov.kz/ru/Api/GetIndexData/703039?period=4&dics=68,848,2513,2854,3068",
        #                  "region", "periods_correlation", "countries", "activity_type", "industrial_products_list", "created_at", "value", "description")
        # self.insert_data("soc_imp_goods_price_index", "Индекс цен на социально-значимые потребительские товары", "week",
        #                 "https://taldau.stat.gov.kz/ru/Api/GetIndexData/18808243?period=2&dics=67,4305,848",
        #                 "region", "social_important_goods", "periods_correlation", "created_at", "value", "description")
        # self.insert_data("avmon_nom_wages", "Среднемесячная номинальная заработная плата", "year",
        #                 "https://taldau.stat.gov.kz/ru/Api/GetIndexData/702972?period=7&dics=68,859,776,2813,576",
        #                 "region", "activity_type", "area_type", "enterprise_dimension", "gender", "created_at", "value", "description")
        # self.insert_data("avmon_nom_wages", "Среднемесячная номинальная заработная плата", "quarter",
        #                 "https://taldau.stat.gov.kz/ru/Api/GetIndexData/702972?period=5&dics=68,859,681",
        #                 "region", "activity_type", "economic_sectors", "created_at", "value", "description")
        # self.insert_data("nom_wages_index", "Индекс номинальной заработной платы", "year",
        #                 "https://taldau.stat.gov.kz/ru/Api/GetIndexData/702974?period=7&dics=68,859,2813,576,848",
        #                 "region", "activity_type", "enterprise_dimension", "gender", "periods_correlation", "created_at", "value", "description")
        # self.insert_data("nom_wages_index", "Индекс номинальной заработной платы", "quarter",
        #                 "https://taldau.stat.gov.kz/ru/Api/GetIndexData/702974?period=5&dics=68,859,2813,848",
        #                 "region", "activity_type", "enterprise_dimension", "periods_correlation", "created_at", "value", "description")
        # self.insert_data("real_wages_index", "Индекс реальной заработной платы", "year",
        #                 "https://taldau.stat.gov.kz/ru/Api/GetIndexData/702976?period=7&dics=68,859,2813,576,848",
        #                 "region", "activity_type", "enterprise_dimension", "gender", "periods_correlation", "created_at", "value", "description")
        # self.insert_data("real_wages_index", "Индекс реальной заработной платы", "quarter",
        #                 "https://taldau.stat.gov.kz/ru/Api/GetIndexData/702976?period=5&dics=68,859,2813,848",
        #                 "region", "activity_type", "enterprise_dimension", "periods_correlation", "created_at", "value", "description")
        # self.insert_data("industry_specialization_sme", "Отраслевая специализация субъектов МСП", "year",
        #                 "https://taldau.stat.gov.kz/ru/Api/GetIndexData/19722414?period=7&dics=67,915,90",
        #                 "region", "activity_type", "enterprise_dimension", "created_at", "value", "description")
        # self.insert_data("labor_statistics", "Статистика труда", "year",
        #                 "https://taldau.stat.gov.kz/ru/Api/GetIndexData/702840?period=7&dics=67,915,749,576,1773,1793",
        #                 "region", "activity_type", "area_type", "gender", "education_level", "age_intervals", "created_at", "value", "description")
        # self.insert_data("labor_statistics", "Статистика труда", "quarter",
        #                 "https://taldau.stat.gov.kz/ru/Api/GetIndexData/702840?period=5&dics=67,2353,749,576,1773,1793",
        #                 "region", "activity_type", "area_type", "gender", "education_level", "age_intervals", "created_at", "value", "description")
        
        # self.insert_data("work_force", "Рабочая сила", "year", 
        #                  "https://taldau.stat.gov.kz/ru/Api/GetIndexData/702835?period=7&dics=67,749,576,1773,1793", 
        #                  "region", "area_type", "gender", "education_level", "age_intervals", "created_at", "value", "description")
        # self.insert_data("work_force", "Рабочая сила", "quarter", 
        #                  "https://taldau.stat.gov.kz/ru/Api/GetIndexData/702835?period=5&dics=67,749,576,1773,1793", 
        #                  "region", "area_type", "gender", "education_level", "age_intervals", "created_at", "value", "description")
    
        # self.insert_data("wage_earners", "Наемные работники", "year", 
        #                  "https://taldau.stat.gov.kz/ru/Api/GetIndexData/702882?period=7&dics=67,915,749,576,1773,1793", 
        #                  "region", "activity_type", "area_type", "gender", "education_level", "age_intervals", "created_at", "value", "description")
        # self.insert_data("wage_earners", "Наемные работники", "quarter", 
        #                  "https://taldau.stat.gov.kz/ru/Api/GetIndexData/702882?period=5&dics=67,915,749,576,1773,1793", 
        #                  "region", "activity_type", "area_type", "gender", "education_level", "age_intervals", "created_at", "value", "description")
        
        # self.insert_data("self_employed", "Самозанятые", "year", 
        #                  "https://taldau.stat.gov.kz/ru/Api/GetIndexData/702935?period=7&dics=67,915,749,576,1773,1793", 
        #                  "region", "activity_type", "area_type", "gender", "education_level", "age_intervals", "created_at", "value", "description")
        # self.insert_data("self_employed", "Самозанятые", "quarter", 
        #                  "https://taldau.stat.gov.kz/ru/Api/GetIndexData/702935?period=5&dics=67,2353,749,576,1773,1793", 
        #                  "region", "activity_type", "area_type", "gender", "education_level", "age_intervals", "created_at", "value", "description")
        
        # self.insert_data("unemployed_number", "Численность безработных", "year", 
        #                  "https://taldau.stat.gov.kz/ru/Api/GetIndexData/702943?period=7&dics=67,749,576,1773,1793", 
        #                  "region", "area_type", "gender", "education_level", "age_intervals", "created_at", "value", "description")
        # self.insert_data("unemployed_number", "Численность безработных", "quarter", 
        #                  "https://taldau.stat.gov.kz/ru/Api/GetIndexData/702943?period=5&dics=67,749,576,1773,1793", 
        #                  "region", "area_type", "gender", "education_level", "age_intervals", "created_at", "value", "description")
        
        # self.insert_data("unemployment_rate", "Уровень безработицы", "year", 
        #                  "https://taldau.stat.gov.kz/ru/Api/GetIndexData/702944?period=7&dics=67,749,576,1773,1793", 
        #                  "region", "area_type", "gender", "education_level", "age_intervals", "created_at", "value", "description")
        
        # self.insert_data("average_wages", "Средняя заработная плата", "year", 
        #                  "https://taldau.stat.gov.kz/ru/Api/GetIndexData/702972?period=7&dics=68,859,776,2813,576", 
        #                  "region", "activity_type", "area_type", "enterprise_dimension", "gender", "created_at", "value", "description")
        # self.insert_data("average_wages", "Средняя заработная плата", "quarter", 
        #                  "https://taldau.stat.gov.kz/ru/Api/GetIndexData/702972?period=5&dics=68,859,681", 
        #                  "region", "activity_type", "economic_sectors", "created_at", "value", "description")
        
        # self.insert_data("avcap_pop_nom_income", "Среднедушевые номинальные денежные доходы населения", "quarter", 
        #                  "https://taldau.stat.gov.kz/ru/Api/GetIndexData/704447?period=5&dics=67", 
        #                  "region", "created_at", "value", "description")
        
        # self.insert_data("pop_income_below_subsistence", "Доля населения, имеющего доходы ниже величины прожиточного минимума", "quarter", 
        #                  "https://taldau.stat.gov.kz/ru/Api/GetIndexData/704498?period=5&dics=67,749", 
        #                  "region", "area_type", "created_at", "value", "description")
        
        # self.insert_data("share_sme", "Доля МСП в ВПП", "year", 
        #                  "https://taldau.stat.gov.kz/ru/Api/GetIndexData/19824647?period=7&dics=67", 
        #                  "region", "created_at", "value", "description")
        # self.insert_data("share_sme_small", "Доля малых МСП в ВПП", "year", 
        #                  "https://taldau.stat.gov.kz/ru/Api/GetIndexData/20380630?period=7&dics=67", 
        #                  "region", "created_at", "value", "description")
        # self.insert_data("share_sme_medium", "Доля средних МСП в ВПП", "year", 
        #                  "https://taldau.stat.gov.kz/ru/Api/GetIndexData/20380634?period=7&dics=67", 
        #                  "region", "created_at", "value", "description")
        # self.insert_data("registered_sme_number", "Количество зарегистрированных субъектов МСП", "year", 
        #                  "https://taldau.stat.gov.kz/ru/Api/GetIndexData/19722414?period=7&dics=67,915,90", 
        #                  "region", "activity_type", "enterprise_dimension", "created_at", "value", "description")
        # self.insert_data("operating_sme_number", "Количество действующих субъектов МСП", "year", 
        #                  "https://taldau.stat.gov.kz/ru/Api/GetIndexData/19722421?period=7&dics=67,915,90", 
        #                  "region", "activity_type", "enterprise_dimension", "created_at", "value", "description")
        # self.insert_data("employees_number", "Численность занятых", "year", 
        #                  "https://taldau.stat.gov.kz/ru/Api/GetIndexData/18714472?period=7&dics=67,915,90", 
        #                  "region", "activity_type", "enterprise_dimension", "created_at", "value", "description")
        # self.insert_data("production_output", "Выпуск продукции", "year", 
        #                  "https://taldau.stat.gov.kz/ru/Api/GetIndexData/701176?period=7&dics=67,915,90", 
        #                  "region", "activity_type", "enterprise_dimension", "created_at", "value", "description")
        # self.insert_data("entities_led_by_under_29", "Количество действующих юр.лиц, руководителями которых является молодежь до 29 лет ", "quarter_accum", 
        #                  "https://taldau.stat.gov.kz/ru/Api/GetIndexData/20612386?period=9&dics=67,915,749,576", 
        #                  "region", "activity_type", "area_type", "gender", "created_at", "value", "description")
        # self.insert_data("entities_led_by_under_35", "Количество действующих юр.лиц, руководителями которых является молодежь до 35 лет", "quarter_accum", 
        #                  "https://taldau.stat.gov.kz/ru/Api/GetIndexData/77218052?period=9&dics=67,915,749,576", 
        #                  "region", "activity_type", "area_type", "gender", "created_at", "value", "description")
        

        # self.insert_data("grp", "Валовый региональный продукт", "year", 
        #                  "https://taldau.stat.gov.kz/ru/Api/GetIndexData/2709379?period=7&dics=67", 
        #                  "region", "created_at", "value", "description")
        # self.insert_data("grp_volume_index", "ИФО для валового внутреннего продукта", "year", 
        #                  "https://taldau.stat.gov.kz/ru/Api/GetIndexData/2979005?period=7&dics=67", 
        #                  "region", "created_at", "value", "description")
        # self.insert_data("grp_per_capita", "Валовый региональный продукт на душу населения", "year", 
        #                  "https://taldau.stat.gov.kz/ru/Api/GetIndexData/2709380?period=7&dics=67", 
        #                  "region", "created_at", "value", "description")
        # self.insert_data("gop_agriculture_forest_fish", "Валовой выпуск продукции (услуг) сельского, лесного и рыбного хозяйства", "year", 
        #                  "https://taldau.stat.gov.kz/ru/Api/GetIndexData/701188?period=7&dics=67,773", 
        #                  "region", "price_measurement", "created_at", "value", "description")
        # self.insert_data("industrial_production_volumes", "Объемы промышленного производства", "year", 
        #                  "https://taldau.stat.gov.kz/ru/Api/GetIndexData/701592?period=7&dics=68,4303", 
        #                  'region', "activity_type", "created_at", "value", "description")
        # self.insert_data("population_of_kazakhstan", "Численность населения Республики Казахстан", "год", 
        #                  "https://taldau.stat.gov.kz/ru/Api/GetIndexData/703831?period=7&dics=67,76", 
        #                  "region", "nationality", "created_at", "value", "description")
        # self.insert_data("population_year_beginning", "Численность населения на начало года", "год", 
        #                  "https://taldau.stat.gov.kz/ru/Api/GetIndexData/703831?period=7&dics=67,749,576,1433", 
        #                  'region', "area_type", "gender", "population_group", "created_at", "value", "description")
        

        
    def collect_data_quarters(self):
        pass


    def collect_data_months(self):
        pass


    def collect_data_weeks(self):
        pass


# automation = Automation("testtest", "postgres", "123456")
# automation.collect_data_years()
# automation.db_disconnect()
