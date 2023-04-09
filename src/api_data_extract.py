# Define a function to extract the data for a specific location and date
import requests
import json
from datetime import datetime

# Define the API endpoint and parameters

url = 'https://api.openweathermap.org/data/2.5/onecall/timemachine'
# add API key
api_key = ''


def extract_data_for_location(location, date):
    # Define the API parameters
    params = {
        'lat': location['lat'],
        'lon': location['lon'],
        'dt': date,
        'appid': api_key,
        'units': 'metric'
    }

    # Make the API request
    response = requests.get(url, params=params)

    # Check the status code
    if response.status_code != 200:
        raise Exception(f'Request failed with status code {response.status_code}')

    # Parse the JSON response
    data = json.loads(response.content)

    # Return the relevant data (you can modify this to extract other fields as needed)
    return {
        'location': location['name'],
        'date': datetime.fromtimestamp(data['current']['dt']).strftime('%Y-%m-%d'),
        'temperature': data['current']['temp']
    }
