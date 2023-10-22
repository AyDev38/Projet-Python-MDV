import requests
from bs4 import BeautifulSoup
import psycopg2
import sqlite3
import time
from datetime import datetime, timedelta

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


def get_episode_duration(episode_url):
    """Récupère la durée de l'épisode à partir de son URL."""
    soup = fetch_web_data(episode_url)
    
    # Cible la div contenant la durée
    duration_div = soup.find('div', class_='episode_infos_episode_format')
    if duration_div:
        # Supprime les espaces en trop
        duration = ' '.join(duration_div.text.strip().split())
        return duration
    else:
        return None


# Fonctions pour SQLite
def create_episode_table_sqlite():
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
    conn.close()

def insert_into_episode_sqlite(data_sorted):
    conn = sqlite3.connect('data/databases/database.db')
    cursor = conn.cursor()
    for item in data_sorted:
        cursor.execute('''
        INSERT INTO episode (date, country, channel, series_name, season, episode, url_episode)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', item)
    conn.commit()
    conn.close()

def create_duration_table_sqlite():
    conn = sqlite3.connect('data/databases/database.db')
    cursor = conn.cursor()
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS duration (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        episode_id INTEGER,
        duration TEXT,
        FOREIGN KEY(episode_id) REFERENCES episode(id)
    )
    ''')
    conn.close()

def insert_duration_sqlite(episode_id, duration):
    conn = sqlite3.connect('data/databases/database.db')
    cursor = conn.cursor()
    cursor.execute('''
    INSERT INTO duration (episode_id, duration)
    VALUES (?, ?)
    ''', (episode_id, duration))
    conn.commit()
    conn.close()

# Fonctions pour PostgreSQL
def create_episode_table_postgresql(database_url):
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
    conn.commit()
    conn.close()

def insert_into_episode_postgresql(data_sorted, database_url):
    conn = psycopg2.connect(database_url, sslmode='require')
    cursor = conn.cursor()
    episode_ids = []
    for item in data_sorted:
        cursor.execute('''
        INSERT INTO episode (date, country, channel, series_name, season, episode, url_episode)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        RETURNING id;
        ''', item)
        episode_id = cursor.fetchone()[0]
        episode_ids.append(episode_id)
    conn.commit()
    conn.close()
    return episode_ids

def create_duration_table_postgresql(database_url):
    conn = psycopg2.connect(database_url, sslmode='require')
    cursor = conn.cursor()
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS duration (
        id SERIAL PRIMARY KEY,
        episode_id INTEGER,
        duration TEXT,
        FOREIGN KEY(episode_id) REFERENCES episode(id)
    )
    ''')
    conn.commit()
    conn.close()

def insert_duration_postgresql(episode_id, duration, database_url):
    conn = psycopg2.connect(database_url, sslmode='require')
    cursor = conn.cursor()
    cursor.execute('''
    INSERT INTO duration (episode_id, duration)
    VALUES (%s, %s)
    ''', (episode_id, duration))
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


# Tentative de résolution de l'exercice : Algorithmie [2/2] non fonctionnelle
# def calculate_consecutive_streaks(data_sorted):
#     streaks = {}  # Dictionnaire pour stocker les séquences consécutives
#     current_streaks = {}  # Dictionnaire pour stocker les séquences actuelles
#     last_seen_date = {}  # Dictionnaire pour stocker la dernière date à laquelle nous avons vu une chaîne

#     # Convertir les chaînes de date en objets datetime pour faciliter la comparaison
#     for date_str, _, channel, *_ in data_sorted:
#         date_parts = date_str.split()
#         if len(date_parts) != 2:  # Vérifie que la date est bien formatée
#             continue  # Ignore les dates mal formatées
#         day, month = date_parts
#         date_obj = datetime.strptime(f"{day} {month} 2023", "%d %B %Y")  # Supposition de l'année 2023

#         if channel not in current_streaks:
#             current_streaks[channel] = 0
#             streaks[channel] = 0
        
#         if channel in last_seen_date:
#             if date_obj - last_seen_date[channel] == timedelta(days=1):  # Si la date est consécutive
#                 current_streaks[channel] += 1
#                 streaks[channel] = max(streaks[channel], current_streaks[channel])
#             else:
#                 current_streaks[channel] = 1
        
#         last_seen_date[channel] = date_obj

#     return streaks


def main():
    print("Début du script...") 

    soup = fetch_web_data(BASE_URL + "calendrier_des_series.html")
    print("Données web récupérées...")  

    data_sorted = extract_series_info(soup)
    print(f"Nombre total d'épisodes récupérés : {len(data_sorted)}")  

    most_common_words = get_most_common_words(data_sorted)
    print("Les 10 mots les plus courants dans les noms de séries :")
    print(most_common_words)
       

    save_to_csv(data_sorted)
    print("Données sauvegardées dans le CSV...")  

    # Pour PostgreSQL
    create_episode_table_postgresql(DATABASE_URL)
    print("Table 'episode' créée dans la base de données PostgreSQL...")
    episode_ids = insert_into_episode_postgresql(data_sorted, DATABASE_URL)
    print("Données insérées dans la table 'episode' de la base de données PostgreSQL...")
    create_duration_table_postgresql(DATABASE_URL)
    print("Table 'duration' créée dans la base de données PostgreSQL...")

    apple_tv_episodes = [episode for episode in data_sorted if episode[2] == "Apple TV+"]

    for episode_id, episode in zip(episode_ids, apple_tv_episodes):
        duration = get_episode_duration(episode[6])
        if duration:
            insert_duration_postgresql(episode_id, duration, DATABASE_URL)
            # print(f"Durée de l'épisode {episode[3]} : {duration}")
    print("Données insérées dans la table 'duration' de la base de données PostgreSQL...")

    # Pour SQLite
    create_episode_table_sqlite()
    print("Table 'episode' créée dans la base de données SQLite...")
    insert_into_episode_sqlite(data_sorted)
    print("Données insérées dans la table 'episode' de la base de données SQLite...")
    create_duration_table_sqlite()
    print("Table 'duration' créée dans la base de données SQLite...")
    for episode_id, episode in zip(range(1, len(apple_tv_episodes) + 1), apple_tv_episodes):
        duration = get_episode_duration(episode[6])
        if duration:
            insert_duration_sqlite(episode_id, duration)

    print("Données insérées dans la table 'duration' de la base de données SQLite...")


    # Tentative de résolution de l'exercice : Algorithmie [2/2] non fonctionnelle
    # streaks = calculate_consecutive_streaks(data_sorted) 

    # if streaks:
    #     max_streak_channel = max(streaks, key=streaks.get)
    #     print(f"La chaîne de TV avec le plus grand nombre de jours consécutifs est : {max_streak_channel} avec {streaks[max_streak_channel]} jours consécutifs.")
    # else:
    #     print("Aucune chaîne de TV n'a été trouvée avec des épisodes consécutifs.")

    print("Fin du script...")

if __name__ == "__main__":
    main()

