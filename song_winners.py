import logging
from bson import ObjectId
from pymongo import MongoClient
from urllib.request import urlopen
from bs4 import BeautifulSoup
from EurovisionStat.winning_eurovision_2019.config import db
from EurovisionStat.winning_eurovision_2019.config.urls import LIST_OF_EUROVISION_SONG_WINNERS


LOGGER = logging.getLogger(__name__)


def parse_by_year(table):
    # type: (BeautifulSoup) -> dict
    """
    Parse html node to dict object.
    Each dict contains years as keys and information about the winner in this year as a content
    :param table: html table
    :return: dict object
    """
    LOGGER.info(f"Parsing winners by year HTML table.")
    winner_by_year = {}
    rows = table.find_all('tr')[1:]
    for row in rows:
        try:
            td = row.find_all('td')
            date = td[0].text.replace('\n', '')
            host_city = td[1].a.text
            if host_city == '':
                host_city = td[1].text.replace('\n', '')
            winner = td[2].a.text
            song = td[3].a.text
            performer = td[4].a.text
            winner_by_year[row.th.a.text.replace('\xa0', '')] = {
                'date': date,
                'host_city': host_city,
                'winner': winner,
                'song': song,
                'performer': performer
            }
        except IndexError:
            pass
        except AttributeError:
            pass
    return winner_by_year


def parse_by_country(table):
    # type: (BeautifulSoup) -> dict
    """
    Parse html node to dict object.
    Each dict contains country as a key and information about wins as a content
    :param table: html table
    :return: dict object
    """
    LOGGER.info(f"Parsing winners by country HTML table.")
    winner_by_country = {}
    rows = table.find_all('tr')[1:]
    for row in rows:
        td = row.find_all('td')
        try:
            country = td[1].text.replace('\n', '')
            wins = td[0].text.replace('\n', '')
            years = [year.text for year in td[2].find_all('a')]
            winner_by_country[country] = {
                'wins': wins,
                'years': years
            }
        except IndexError:
            pass
        except AttributeError:
            pass
    return winner_by_country


def parse_by_lang(table):
    # type: (BeautifulSoup) -> dict
    """
    Parse html node to dict object.
    Each dict contains language as a key and information about winners in this language as a content
    :param table: html table
    :return: dict object
    """
    LOGGER.info(f"Parsing winners by language HTML table.")
    winner_by_lang = {}
    rows = table.find_all('tr')[1:]
    for row in rows:
        td = row.find_all('td')
        try:
            lang = td[1].a.text
            wins = td[0].text.replace('\n', '')
            years = [year.text for year in td[2].find_all('a')]
            countries = [year.text for year in td[3].find_all('a')]
            winner_by_lang[lang] = {
                'wins': wins,
                'years': years,
                'Countries': countries
            }
        except IndexError:
            pass
        except AttributeError:
            pass
    return winner_by_lang


def insert_to_db(documents):
    """
    Insert document or list of document (dict or JSON) to db
    :param documents: dict or list of dicts who represent wikipedia table
    :return: None
    :raise: TypeError if documents not in correct type (dict or list)
    """
    client = MongoClient(f'mongodb://{db.USERNAME}:{db.PASSWORD}@{db.HOST}:{db.PORT}/{db.NAMESPACE}')
    eurovision_db = client.eurovision
    LOGGER.info(f"Save documents to collection: wikipedia")
    if type(documents) is dict:
        documents = [documents]
    if type(documents) is list:
        for doc in documents:
            wikipedia = eurovision_db.wikipedia
            wikipedia.insert_one(doc)
    else:
        raise TypeError


def download_html(url):
    # type: (str) -> BeautifulSoup
    """
    Download html from given url and return BeautifulSoup for parse the html
    :param url: url for the wanted html
    :return: BeautifulSoup object with the downloaded html content
    """
    LOGGER.info(f"Downloading HTML content. URL: {url}")
    content = urlopen(url).read()
    return BeautifulSoup(content, 'html.parser')


def parse_winner_tables(node):
    # type: (BeautifulSoup) -> list[dict]
    """
    Parse winner tables from given html node
    :param node: BeautifulSoup object with winner tables inside
    :return: list of parsed tables -> from html to Python dict
    """
    winners_tables = node.find_all("table", {'class': 'wikitable'})
    winner_by_year = parse_by_year(winners_tables[0])
    winner_by_country = parse_by_country(winners_tables[1])
    winner_by_lang = parse_by_lang(winners_tables[4])
    return [winner_by_year, winner_by_country, winner_by_lang]


def rebind_songs():
    """
    Bind songs data from spotify to song name in winner collection
    Insert binding data to new collection
    :return: None
    """
    client = MongoClient(f'mongodb://{db.USERNAME}:{db.PASSWORD}@{db.HOST}:{db.PORT}/{db.NAMESPACE}')
    eurovision_db = client.eurovision
    LOGGER.info(f"Get spotify songs")
    winners_by_year_new = {}
    old_winners = db.ALL_WINNERS_BY_YEAR
    for year in old_winners:
        old_winner_song = old_winners[year]['song'].lower()
        spotify_songs = eurovision_db["winners_songs_spotify"].find({})
        for song in spotify_songs:
            if song['name'].lower() == old_winner_song:
                old_winners[year]['song'] = song
                winners_by_year_new[year] = old_winners[year]
                break
    eurovision_db['winner_by_year_new'].insert_one(winners_by_year_new)


def get_songs_statistics():
    """
    Calculate song statistic from all winners songs over the year
    :return: dict object with calculated data
    """
    client = MongoClient(f'mongodb://{db.USERNAME}:{db.PASSWORD}@{db.HOST}:{db.PORT}/{db.NAMESPACE}')
    eurovision_db = client.eurovision
    LOGGER.info(f"Get winners collection")
    winners_collection = eurovision_db["winner_by_year_new"].find_one({})
    lang_coll = eurovision_db["wikipedia"].find()[1]
    lang_coll.pop('_id', None)
    for lang in lang_coll:
        try:
            years = lang_coll[lang]['years']
            for year in years:
                for winner_year in winners_collection:
                    if year == winner_year:
                        winner = winners_collection[year]
                        winner['song']['language'] = lang.lower()
        except:
            pass
    print(f"winner: {winners_collection}")
    eurovision_db["winner_by_year_new"].update_one({}, {"$set": winners_collection})
    winners_collection = eurovision_db["winner_by_year_new"].find_one({})
    winners_collection.pop('_id', None)
    songs_statistics = {
        'key': {},
        'lang': {
            'english': 0,
            'other': 0
        },
        "composition": {
            'band': 0,
            'solo': 0
        },
        "genre": {
            'pop': 0,
            'classic': 0,
            'rock': 0,
            'other': 0
        }
    }
    all_songs = 0
    LOGGER.info(f"Getting tune statistics")
    for year in winners_collection:
        winner = winners_collection[year]
        try:
            song_key = winner['song']['key']
            song_lang = winner['song']['language']
            composition = winner['song']['artist']
            song_genres = winner['song']['genres'][0]
            for genre in song_genres:
                genre_lower = genre.lower()
                if 'pop' in genre_lower:
                    songs_statistics['genre']['pop'] += 1
                elif 'classic' in genre_lower:
                    songs_statistics['genre']['classic'] += 1
                elif 'rock' in genre_lower:
                    songs_statistics['genre']['rock'] += 1
                else:
                    songs_statistics['genre']['other'] += 1
            if song_lang == 'english':
                songs_statistics['lang'][song_lang] += 1
            else:
                songs_statistics['lang']['other'] += 1
            if len(composition) > 1:
                songs_statistics['composition']['band'] += 1
            else:
                songs_statistics['composition']['solo'] += 1

            try:
                songs_statistics['key'][song_key] += 1
            except KeyError:
                songs_statistics['key'][song_key] = 1
            finally:
                all_songs += 1
        except KeyError:
            print(f"error: {winner}")
    eurovision_db['songs_statistic'].insert_one(songs_statistics)
    return songs_statistics


def workflow():
    get_songs_statistics()
