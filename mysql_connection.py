import requests
import json
import pandas as pd

# api_url = "https://dummyapi.io/data/v1/user/?page=0&limit=50"
parameters = {
    "app-id": "629c7216cdf998caab5c026f"
}

response = requests.get("https://dummyapi.io/data/v1/user/?page=1&limit=10", headers=parameters)
data_info = json.loads(response.text)['data']
df = pd.DataFrame(data_info)


for i in df['id']:
    response = requests.get("https://dummyapi.io/data/v1/user/" + i + "", headers=parameters)
    df_temp = json.loads(response.text)
    df_data = pd.DataFrame([df_temp])
    df_data['loc_street'] = df_data['location'].str['street']
    df_data['loc_city'] = df_data['location'].str['city']




# a = pd.json_normalize(df_data['location'], max_level=1)
# result = pd.concat([df_data, a], axis=1)
print(df_data.head(10))
