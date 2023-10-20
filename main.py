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
        country_img = serie.find_previous('img', alt=True)
        country = country_img['alt']
        channel_img = country_img.find_next('img', alt=True)
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
        channel TEXT,
        country TEXT,
        series_name TEXT,
        season INTEGER,
        episode INTEGER,
        url_episode TEXT
    )
    ''')
    for item in data_sorted:
        cursor.execute('''
        INSERT INTO episode (date, channel, country, series_name, season, episode, url_episode)
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
        channel TEXT,
        country TEXT,
        series_name TEXT,
        season INTEGER,
        episode INTEGER,
        url_episode TEXT
    )
    ''')
    for item in data_sorted:
        cursor.execute('''
        INSERT INTO episode (date, channel, country, series_name, season, episode, url_episode)
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

def extract_episode_duration(data_sorted, base_url, channel_filter="Apple TV+"):
    """Extrait la durée des épisodes pour une chaîne spécifique."""
    filtered_episodes = [item for item in data_sorted if item[2] == channel_filter]
    
    for episode in filtered_episodes:
        url_episode = episode[6]
        soup = fetch_web_data(url_episode)
        
        # Cible la div contenant la durée
        duration_div = soup.find('div', class_='episode_infos_episode_format')
        if duration_div:
            # Ici, nous extrayons le texte, le nettoyons et le joignons pour obtenir un format "25 minutes".
            duration = ' '.join(duration_div.text.strip().split())
            episode.append(duration)
        else:
            episode.append(None)
        
        time.sleep(2)  # Attend 2 secondes avant de faire une autre requête
    return filtered_episodes

def display_data(episodes):
    """Affiche les données des épisodes à la console."""
    for episode in episodes:
        date, country, channel, series_name, season, episode_num, url_episode, duration = episode
        print(f"{series_name} (Saison {season} Épisode {episode_num}) - {duration} - {url_episode}")

def main():
    print("Début du script...")  # Nouveau print pour le débogage

    soup = fetch_web_data(BASE_URL + "calendrier_des_series.html")
    print("Données web récupérées...")  # Nouveau print pour le débogage

    data_sorted = extract_series_info(soup)
    print(f"Nombre total d'épisodes récupérés : {len(data_sorted)}")  # Nouveau print pour le débogage

    # save_to_csv(data_sorted)
    # print("Données sauvegardées dans le CSV...")  # Nouveau print pour le débogage

    # save_to_sqlite(data_sorted)
    # save_to_postgresql(data_sorted, DATABASE_URL)

    most_common_words = get_most_common_words(data_sorted)
    print(most_common_words)

    channels = set([item[2] for item in data_sorted])
    print(f"Toutes les chaînes disponibles : {channels}")

    apple_tv_episodes = extract_episode_duration(data_sorted, BASE_URL)
    print(f"Nombre d'épisodes pour Apple TV : {len(apple_tv_episodes)}")  # Nouveau print pour le débogage

    display_data(apple_tv_episodes)

if __name__ == "__main__":
    main()

