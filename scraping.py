import requests
from bs4 import BeautifulSoup
import csv
import psycopg2


def main():
    data, unique_events = get_scraping_data()
    create_csv(data)
    create_insert_data(data, unique_events)


def get_scraping_data():
    data = []
    unique_events = set()

    # пробегаюсь по всем месяцам и годам
    for year in range(2021, 2024):
        for month in range(1, 13):
            url = f'https://stat.gov.kz/ru/release-calendar/?date={month}.{year}'

            # получаю содрежимое страницы
            response = requests.get(url)

            if response.status_code == 200:
                # Получаем содержимое страницы calendar-event-title
                soup = BeautifulSoup(response.text, 'html.parser')
                dates = soup.find_all(class_="calendar-event-day")
                events = soup.find_all(class_="calendar-event-title")
                
                for date, event in zip(dates, events):
                    # форматирование текста
                    event_name = event.text.strip()
                    event_date = date.text.strip()
                    try:
                        index_bracket = [index for index, symbol in enumerate(event_name) if symbol == "("][-1]
                    except:
                        index_bracket = len(event_name)
                    new_event = [event_date, event_name[:index_bracket].strip(), event_name[index_bracket:]]
                    data.append(new_event)
                    unique_events.add(event_name[:index_bracket].strip())

            else:
                print('Не удалось получить доступ к странице.')
    
    return data, unique_events


def create_csv(data):
    with open("event_data.csv", 'a', newline='', encoding='utf-8') as file:
        csvwriter = csv.writer(file)
        for row in data:
            csvwriter.writerow(row)


def create_insert_data(data, unique_events):
    conn = psycopg2.connect("dbname=releases user=postgres password=123456")
    cur = conn.cursor()
    create_table_publications = """
    CREATE TABLE publications (
        id SERIAL PRIMARY KEY,
        name VARCHAR(500)
    );
    """

    create_table_info = """
    CREATE TABLE info (
        id SERIAL PRIMARY KEY,
        date DATE,
        description VARCHAR(500),
        publication_id INT REFERENCES publications(id)
    );
    """

    cur.execute(create_table_publications)
    cur.execute(create_table_info)
    

    for event in unique_events:
        insert_query = f"""
        INSERT INTO publications (name)
        VALUES (%s);
        """
        cur.execute(insert_query, (event,))


    for row in data:
        date = row[0]
        name = row[1]
        description = row[2]
        print(date, name, description)
        insert_query = """
        INSERT INTO info (date, description, publication_id)
        VALUES (TO_DATE(%s, 'DD.MM.YYYY'), %s, (SELECT id FROM publications WHERE name = %s));
        """
        cur.execute(insert_query, (date, description, name))


    conn.commit()
    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
