import requests
from urllib.request import urlopen
from bs4 import BeautifulSoup
import logging
from pymongo import MongoClient
from EurovisionStat.winning_eurovision_2019.config import db
from EurovisionStat.winning_eurovision_2019.config.urls import EUROVISION_DB_URL, VOTES_URL

client = MongoClient(f'mongodb://{db.USERNAME}:{db.PASSWORD}@{db.HOST}:{db.PORT}/{db.NAMESPACE}')

LOGGER = logging.getLogger(__name__)


def get_all_countries():
    # type: () -> dict
    """
    Get all countries in eurovision database
    :return: key: value pairs --> 2 first letters in country name : full country name
    """
    LOGGER.info("Get all countries from countries list")
    bs = BeautifulSoup(urlopen(EUROVISION_DB_URL).read(), 'html.parser')
    countries_container = bs.find('select', {'id': 'nosubmit', 'name': 'country_x'}).find_all('option')
    country_list = {}
    for _country in countries_container:
        country_list[_country['value']] = _country.text
    return country_list


def get_all_votes(country, from_country=False, year_from=1957, year_to=2018):
    # type: (str, bool, int, int) -> list[dict]
    """
    Get all votes from country or to country.
    for example:
    for country = Israel:
        get all votes from Israel to other countries (from = True)
        get all voter to Israel from other countries (from = False)
    :param year_from: From which year get result
    :param year_to: To which year get result
    :param country: country to check votes on
    :param from_country: True -> votes FROM this country, False -> votes TO this country
    :return:list of {country: votes} pairs (dict)
    """
    direction = 0
    if from_country:
        LOGGER.info(f"Get all votes from country. country: {country}")
        direction = 1
    else:
        LOGGER.info(f"Get all votes to country. country: {country}")
    payload = {
        'art': 0,
        'direction': direction,
        'country_x': country,
        'year_from': year_from,
        'year_to': year_to,
        'x': 5,
        'y': 7
    }
    headers = {'content-type': 'application/x-www-form-urlencoded'}
    result = requests.post(f'{EUROVISION_DB_URL}{VOTES_URL}', data=payload, headers=headers)
    bs = BeautifulSoup(result.text, 'html.parser')
    table = bs.find("table", {"id": "tabelle1"})
    rows = table.find_all("tr")
    all_votes = []
    for row in rows:
        try:
            votes = {
                'country': row.find_all('td')[1].text.replace(' ', ''),
                'points': row.find_all('td')[2].text.replace(' ', '')
            }
            all_votes.append(votes)
        except AttributeError:
            pass
        except KeyError:
            pass
        except TypeError:
            pass
        except IndexError:
            pass
    return all_votes


def insert_to_db(client, documents, collection_name):
    """
    Insert document or list of document (dict or JSON) to db
    :param client: Mongo client
    :param collection_name: collection to store the documens
    :param documents: dict or list of dicts who represent wikipedia table
    :return: None
    :raise: TypeError if documents not in correct type (dict or list)
    """
    eurovision_db = client.eurovision
    if type(documents) is dict:
        documents = [documents]
    if type(documents) is list:
        for doc in documents:
            LOGGER.info(f"Insert new document to collection {collection_name}")
            collection = eurovision_db[collection_name]
            collection.insert_one(doc)
    else:
        raise TypeError


def workflow():
    LOGGER.info(f"Start downloading votes statistics from url: {EUROVISION_DB_URL}")

    countries = get_all_countries()
    # For each country get all country who votes for her
    all_points_given_to = []
    for country in countries:
        all_points_given_to.append(
            {
                'country': countries[country],
                'get_votes_from': get_all_votes(country)
            }
        )
    # For each country get all country that she votes for (direction - to: 0, from: 1)
    all_points_given_from = []
    for country in countries:
        all_points_given_from.append(
            {
                'country': countries[country],
                'voted_to': get_all_votes(country, from_country=True)
            }
        )
    # Store data to mongodb collection
    insert_to_db(client, all_points_given_from, 'all_points_given_from')
    insert_to_db(client, all_points_given_to, 'all_points_given_to')
