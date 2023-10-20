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
# for item in data_sorted:
    # print(item)

def simple_type(value):
    t = type(value).__name__
    if t == 'str':
        return 'string'
    if t == 'int':
        return 'integer'
    if t == 'float':
        return 'float'


# Enregistrez les données dans un fichier CSV
with open('data/files/episodes.csv', 'w', encoding='utf-8') as file:
    for item in data_sorted:
        series_name = item[3]
        episode = int(item[5])
        season = int(item[4])
        url_episode = serie.find('a', class_='liens')['href']
        file.write(f"{series_name};{episode};{season};{url_episode} # {simple_type(series_name)} {simple_type(episode)} {simple_type(season)} {simple_type(url_episode)}\n")


def read_episodes(filename):
    # Ouvrir le fichier en mode lecture
    with open(filename, 'r', encoding='utf-8') as file:
        # Lire toutes les lignes
        lines = file.readlines()

    # Ignorer la première ligne (l'en-tête)
    lines = lines[1:]

    # Liste pour stocker les épisodes
    episodes = []

    for line in lines:
        # Supprimer les caractères d'espacement en début et fin de ligne
        line = line.strip()

        # Séparer la ligne en utilisant le séparateur ";"
        parts = line.split(";")

        # Convertir les données aux types appropriés
        name = parts[0]
        episode = int(parts[1])
        season = int(parts[2])
        url = parts[3]

        # Ajouter le tuple à la liste
        episodes.append((name, episode, season, url))

    return episodes

# Test de la fonction
episodes_data = read_episodes('data/files/episodes.csv')
for episode in episodes_data:
    print(episode)