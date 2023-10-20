import requests
from bs4 import BeautifulSoup

url = "https://www.spin-off.fr/calendrier_des_series.html"

response = requests.get(url)
soup = BeautifulSoup(response.content, 'html.parser')

# Récupérer toutes les dates
dates_divs = soup.find_all('div', class_='div_jour')
dates = [date_div.find('div').text.strip() + date_div.contents[1].strip() for date_div in dates_divs]

# Extraire les informations sur chaque série
series_info = soup.find_all('span', class_='calendrier_episodes')

data = []

for i, serie in enumerate(series_info):
    country_img = serie.find_previous('img', alt=True)
    country = country_img['alt']

    channel_img = country_img.find_next('img', alt=True)
    channel = channel_img['alt']

    series_name = serie.find('a').text
    episode_details = serie.find('a', class_='liens').text

    # Récupération de la saison et de l'épisode
    season, episode = episode_details.split('.')
    data.append((dates[i % len(dates)], country, channel, series_name, season, episode))

# Trier les données en fonction des numéros de jour
data_sorted = sorted(data, key=lambda x: int(''.join(filter(str.isdigit, x[0]))))

# Afficher les données triées
for item in data_sorted:
    print(item)
