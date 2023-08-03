import requests

def get_index_price():
    url = 'https://taldau.stat.gov.kz/ru/Api/GetIndexData/703053?period=5&dics=67,848,2815' 
    response = requests.post(url) 

    if response.status_code == 200:
        data = response.json()
        categories = []
        kz_data = []
        filter_data = []
        for row in data:
            if row["terms"][0] == 741880:
                kz_data.append(row)
        
        for row in kz_data:
            categories.append(row["termNames"][2])

        for row in kz_data:
            unit_data = []
            for period in row["periods"]:
                unit_data = [int(period["name"][0]), period["date"], period["value"], row["termNames"][2]]
                filter_data.append(unit_data)
        
    else:
        print(f"Ошибка запроса: {response.status_code}")

    return categories, filter_data
