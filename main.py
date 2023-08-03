import psycopg2
import requests

#gdp_production_method
###################

def get_index_price():
    url = 'https://taldau.stat.gov.kz/ru/Api/GetIndexData/2709379?period=7&dics=67' 
    response = requests.post(url) 

    if response.status_code == 200:
        data = response.json()
        regions = set()
        kz_data = []
        filter_data = []
        for row in data:
            if row["terms"][0] != 741880:
                kz_data.append(row)
        
        for row in kz_data:
            regions.add(row["termNames"][0])

        for row in kz_data:
            unit_data = []
            for period in row["periods"]:
                unit_data = [period["date"], period["value"], row["termNames"][0]]
                filter_data.append(unit_data)
        
    else:
        print(f"Ошибка запроса: {response.status_code}")

    return regions, filter_data


def create_insert_data(list_of_regions, list_of_indexes):
    conn = psycopg2.connect("dbname=statistics user=postgres password=123654")
    cur = conn.cursor()
    create_table_regions = """
    CREATE TABLE regions (
        id SERIAL PRIMARY KEY,
        name VARCHAR(200)
    );
    """

    create_table_gdp_production_method = """
    CREATE TABLE gdp_production_method (
        id SERIAL PRIMARY KEY,
        date DATE,
        value BIGINT,
        updated DATE,
        region_id INT REFERENCES regions(id)
    );
    """

    cur.execute(create_table_regions)
    cur.execute(create_table_gdp_production_method)
    

    for element in list_of_regions:
        insert_query = """
        INSERT INTO regions (name)
        VALUES (%s);
        """
        cur.execute(insert_query, (element,))


    for row in list_of_indexes:
        date = row[0]
        value = row[1]
        name = row[2]

        insert_query = """
        INSERT INTO gdp_production_method (date, value, region_id)
        VALUES (TO_DATE(%s, 'DD.MM.YYYY'), %s, (SELECT id FROM regions WHERE name = %s));
        """
        cur.execute(insert_query, (date, value, name))


    conn.commit()
    cur.close()
    conn.close()


#######################