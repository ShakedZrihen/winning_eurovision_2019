import logging
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


def workflow():
    html = download_html(LIST_OF_EUROVISION_SONG_WINNERS)
    winner_tables = parse_winner_tables(html)
    insert_to_db(winner_tables)
