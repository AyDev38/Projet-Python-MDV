import requests
from bs4 import BeautifulSoup
import psycopg2
import sqlite3

BASE_URL = "https://www.spin-off.fr/"

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
        INSERT INTO episode (date, country, channel, series_name, season, episode, url_episode)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ''', item)
    conn.commit()
    conn.close()


def main():
    soup = fetch_web_data(BASE_URL + "calendrier_des_series.html")
    data_sorted = extract_series_info(soup)
    save_to_csv(data_sorted)
    save_to_sqlite(data_sorted)
    DATABASE_URL = 'postgres://cours_pytho_5421:F7XnoI1TH_KbiiwxghBE@cours-pytho-5421.postgresql.a.osc-fr1.scalingo-dbs.com:33800/cours_pytho_5421?sslmode=prefer'
    save_to_postgresql(data_sorted, DATABASE_URL)


if __name__ == "__main__":
    main()
