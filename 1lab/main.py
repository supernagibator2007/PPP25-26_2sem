import requests
import sqlite3
from datetime import datetime


URL_1 = 'https://jsonplaceholder.typicode.com/posts'
URL_2 = 'https://jsonplaceholder.typicode.com/users'
DATA_BASE = 'posts.db'
DATE_FORMAT = '%Y/%m/%d %H:%M:%S'


def extract(URL_1, URL_2):
    response_1 = requests.get(URL_1).json()
    response_2 = requests.get(URL_2).json()
    return response_1, response_2


def transform(people, posts):
    unified_data = []
    users = {person['id']: person['name'] for person in people}
    for post in posts:
        unified = {
            'id': post['id'],
            'title': post['title'],
            'body': post['body'],
            'author': users.get(post['userId'], '-'),
            'extracted_time': datetime.now().strftime(DATE_FORMAT)
        }
        unified_data.append(unified)

    return unified_data


def load(posts):
    conn = sqlite3.connect(DATA_BASE)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS posts (
            id INTEGER PRIMARY KEY,
            title TEXT,
            body TEXT,
            author TEXT,
            extracted_time TEXT
        )
    ''')
    cursor.execute('DELETE FROM posts')
    for post in posts:
        cursor.execute('''
            INSERT INTO posts (id, title, body, author, extracted_time)
            VALUES (?, ?, ?, ?, ?)
        ''', (post['id'], post['title'], post['body'],
              post['author'], post['extracted_time'])
        )
    conn.commit()
    conn.close()


def run_etl():
    raw_posts, raw_users = extract(URL_1, URL_2)
    transformed = transform(raw_users, raw_posts)
    load(transformed)


if __name__ == '__main__':
    conn = sqlite3.connect(DATA_BASE)
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT * FROM posts LIMIT 1")
        row = cursor.fetchone()
        if (datetime.now() - datetime.strptime(row[4], DATE_FORMAT)).total_seconds() >= 30:
            run_etl()
            print('Обновлено')
    except sqlite3.OperationalError:
        run_etl()
        print('Записано')
