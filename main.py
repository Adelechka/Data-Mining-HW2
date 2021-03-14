import asyncio
import re

import psycopg2
import pymorphy2
from nltk.corpus import stopwords
from psycopg2 import sql
from vk_api import *

TOKEN = '9699e9069699e9069699e9066b96eb12ee996999699e906c9916b638650d9757c059069'
DOMAIN = 'itis_kfu'
POSTS_COUNT = 200
COUNT = 100

DBNAME = 'postgres'
USER = 'postgres'
PASSWORD = 'qwerty007'
HOST = 'database-hw2.c4mika4kztfi.us-east-1.rds.amazonaws.com'
PORT = '5432'


def get_items(vk_connection, offset):
    return vk_connection.wall.get(domain=DOMAIN, count=COUNT, offset=offset).get('items')


def reformat_post(post_for_processing):
    post_for_processing = re.sub("\\n", " ", post_for_processing)
    post_for_processing = re.sub("[^a-zA-Zа-яА-ЯёЁ#_ ]", "", post_for_processing).lower()
    return post_for_processing


def count_unique(words_for_count, counter):
    morph1 = pymorphy2.MorphAnalyzer()
    for word in range(len(words_for_count)):
        if not words_for_count[word] in stopwords.words('russian'):
            if words_for_count[word] != '':
                if not re.fullmatch('(#.+)', words_for_count[word]):
                    words_for_count[word] = morph1.parse(words_for_count[word])[0].normal_form
                if counter.get(words_for_count[word]) is None:
                    counter.setdefault(words_for_count[word], 1)
                else:
                    counter[words_for_count[word]] = counter[words_for_count[word]] + 1


def save_to_database(counter):
    conn = psycopg2.connect(dbname=DBNAME, user=USER, password=PASSWORD, host=HOST, port=PORT)
    with conn.cursor() as cursor:
        conn.autocommit = True
        cursor.execute('DROP TABLE IF EXISTS stat')
        cursor.execute('create table stat (word varchar(60), count integer)')
        keys = list(counter.keys())
        values = list(counter.values())
        for k in range(100):
            value = (keys[k], values[k])
            print(value)
            insert = sql.SQL('INSERT INTO stat (word, count) VALUES ({})').format(
                sql.SQL(',').join(map(sql.Literal, value))
            )
            cursor.execute(insert)
    cursor.close()
    conn.close()


async def main():
    COUNTER = {}
    OFFSET = 0
    print('Analysis started')

    vk_session = vk_api.VkApi(token=TOKEN)
    vk = vk_session.get_api()

    for i in range(POSTS_COUNT // 100):
        items = get_items(vk, OFFSET)
        for j in range(COUNT):
            post = items[j].get('text')
            post = reformat_post(post)
            words = post.split(" ")
            count_unique(words, COUNTER)
        OFFSET += 100

    COUNTER = {k: v for k, v in sorted(COUNTER.items(),
                                       key=lambda item: item[1],
                                       reverse=True)}

    save_to_database(COUNTER)

    print('End of work')


asyncio.run(main())
