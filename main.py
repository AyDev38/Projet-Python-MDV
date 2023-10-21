import requests
from bs4 import BeautifulSoup
import psycopg2
import sqlite3
import time

BASE_URL = "https://www.spin-off.fr/"
DATABASE_URL = "postgres://cours_pytho_5421:F7XnoI1TH_KbiiwxghBE@cours-pytho-5421.postgresql.a.osc-fr1.scalingo-dbs.com:33800/cours_pytho_5421?sslmode=prefer"

def fetch_web_data(url):
    """Récupère les données du site web."""
    response = requests.get(url)
    return BeautifulSoup(response.content, 'html.parser')


def extract_series_info(soup):
    """Extrait les informations des séries à partir du BeautifulSoup passé en paramètre."""
    dates_divs = soup.find_all('div', class_='div_jour')
    dates = [date_div.find('div').text.strip() + date_div.contents[1].strip() for date_div in dates_divs]
    series_info = soup.find_all('span', class_='calendrier_episodes')
    
    data = []
    for i, serie in enumerate(series_info):
        country_img = serie.find_next('img', alt=True)
        country = country_img['alt']
        channel_img = country_img.find_previous('img', alt=True)
        channel = channel_img['alt']
        series_name = serie.find('a').text
        episode_details = serie.find('a', class_='liens').text
        url_episode = serie.find('a', class_='liens')['href']
        season, episode = episode_details.split('.')
        data.append((dates[i % len(dates)], country, channel, series_name, season, episode, BASE_URL + url_episode))
    
    return sorted(data, key=lambda x: int(''.join(filter(str.isdigit, x[0]))))


def save_to_csv(data_sorted):
    """Sauvegarde les données dans un fichier CSV."""
    with open('data/files/episodes.csv', 'w', encoding='utf-8') as file:
        for item in data_sorted:
            series_name, episode, season, url_episode = item[3], int(item[5]), int(item[4]), item[6]
            file.write(f"{series_name};{episode};{season};{url_episode} # {simple_type(series_name)} {simple_type(episode)} {simple_type(season)} {simple_type(url_episode)}\n")


def simple_type(value):
    """Renvoie le type simplifié d'une valeur."""
    t = type(value).__name__
    return {
        'str': 'string',
        'int': 'integer',
        'float': 'float'
    }.get(t, t)


def save_to_sqlite(data_sorted):
    """Sauvegarde les données dans une base de données SQLite."""
    conn = sqlite3.connect('data/databases/database.db')
    cursor = conn.cursor()
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS episode (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        date TEXT,
        country TEXT,
        channel TEXT,
        series_name TEXT,
        season INTEGER,
        episode INTEGER,
        url_episode TEXT
    )
    ''')
    for item in data_sorted:
        cursor.execute('''
        INSERT INTO episode (date, country, channel, series_name, season, episode, url_episode)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', item)
    conn.commit()
    conn.close()


def save_to_postgresql(data_sorted, database_url):
    """Sauvegarde les données dans une base de données PostgreSQL."""
    conn = psycopg2.connect(database_url, sslmode='require')
    cursor = conn.cursor()
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS episode (
        id SERIAL PRIMARY KEY,
        date TEXT,
        country TEXT,
        channel TEXT,
        series_name TEXT,
        season INTEGER,
        episode INTEGER,
        url_episode TEXT
    )
    ''')
    for item in data_sorted:
        cursor.execute('''
        INSERT INTO episode (date, country, channel, series_name, season, episode, url_episode)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ''', item)
    conn.commit()
    conn.close()

def get_most_common_words(data_sorted):
    """Renvoie les 10 mots les plus courants dans les noms de séries."""
    from collections import Counter
    import re

    # Récupère tous les noms de séries
    series_names = [item[3] for item in data_sorted]

    # Divise chaque nom de série en mots et les met en minuscules
    words = [word.lower() for name in series_names for word in re.findall(r'\w+', name)]
    word_counts = Counter(words)
    return word_counts.most_common(10)

def get_episode_duration(episode_url):
    """Récupère la durée de l'épisode à partir de son URL."""
    soup = fetch_web_data(episode_url)
    
    # Cible la div contenant la durée
    duration_div = soup.find('div', class_='episode_infos_episode_format')
    if duration_div:
        # Ici, nous extrayons le texte, le nettoyons et le joignons pour obtenir un format "25 minutes".
        duration = ' '.join(duration_div.text.strip().split())
        return duration
    else:
        return None


def main():
    print("Début du script...") 

    soup = fetch_web_data(BASE_URL + "calendrier_des_series.html")
    print("Données web récupérées...")  

    data_sorted = extract_series_info(soup)
    print(f"Nombre total d'épisodes récupérés : {len(data_sorted)}")  

    # Filtrage des épisodes pour Apple TV+
    apple_tv_episodes = [episode for episode in data_sorted if episode[2] == "Apple TV+"]
    
    # Récupération de la durée pour chaque épisode d'Apple TV+
    for episode in apple_tv_episodes:
        duration = get_episode_duration(episode[6])
        episode = episode + (duration,)  # Ajout de la durée à l'épisode
        time.sleep(2)  # Attend 2 secondes avant de faire une autre requête
        print(f"Durée de l'épisode {episode[3]} : {duration}")
        

    # save_to_csv(data_sorted)
    # print("Données sauvegardées dans le CSV...")  

    # save_to_sqlite(data_sorted)
    # print("Données sauvegardées dans dans la base sqlite...")

    # save_to_postgresql(data_sorted, DATABASE_URL)
    # print("Données sauvegardées dans la base Scallingo...")

    # most_common_words = get_most_common_words(data_sorted)
    # print("Les 10 mots les plus courants dans les noms de séries :")
    # print(most_common_words)


if __name__ == "__main__":
    main()

