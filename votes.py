import requests
from urllib.request import urlopen
from bs4 import BeautifulSoup
import logging
from pymongo import MongoClient
from EurovisionStat.winning_eurovision_2019.config import db
from EurovisionStat.winning_eurovision_2019.config.urls import EUROVISION_DB_URL, VOTES_URL, VOTES_FROM, VOTES_TO

client = MongoClient(f'mongodb://{db.USERNAME}:{db.PASSWORD}@{db.HOST}:{db.PORT}/{db.NAMESPACE}')

LOGGER = logging.getLogger(__name__)


def create_country_flag_collection():
    # type: ()-> ()
    """
    Create document where the country is the key & flag's picture is the value
    :return: Nothing
    """
    countries = get_all_countries()
    country_flag = {}
    for country in countries:
        country_flag[countries[country]] = 'https://eschome.net/flags/' + country + '.png'
    insert_to_db(client, country_flag, 'country_flag')


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
        except Exception as e:
            print(e)
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

    # For each country get all country that she votes for (direction - to: 0, from: 1)
    all_points_given_from = []
    for country in countries:
        for year in range(1956, 2019):
            points_year_from = {
                'year': year,
                'country': countries[country].lower(),
                'voted': get_all_votes_from_specific_year(country, year, from_country=True)
            }
            all_points_given_from.append(points_year_from)
            # Store data to mongodb collection
            insert_to_db(client, points_year_from, 'points_by_year_given_from')

    # For each country get all country who votes for her
    all_points_given_to = []
    for country in countries:
        for year in range(1956, 2019):
            points_year_to = {
                    'year': year,
                    'country': countries[country].lower(),
                    'voted': get_all_votes_from_specific_year(country, year)
                }
            all_points_given_to.append(points_year_to)
            insert_to_db(client, points_year_to, 'points_by_year_given_to')


def calc_best_friends():
    """
    Calculate top best friend - top 3 pairs of countries that voted the most to each other over years
    :return:
    """
    total_countries_votes = client.eurovision['all_points_given_from'].find()
    countries_sorted_value = {}
    bff = []
    for country in total_countries_votes:
        countries_sorted_value[country['country']] = sorted(country['voted_to'], key=lambda k: int(k['points']), reverse=True)[:4]
    for country in countries_sorted_value:
        try:
            country_given_to = countries_sorted_value[country][0]['country']
            country_given_from = countries_sorted_value[countries_sorted_value[country][0]['country']][0]['country']
            if country_given_from == country:
                if (country_given_to, country) not in bff:
                    bff.append((country, country_given_to))
        except KeyError:
            pass
    if len(bff) > 3:
        bff_by_score = {}
        for pair in bff:
            bff_by_score[int(countries_sorted_value[pair[0]][0]['points']) + int(countries_sorted_value[pair[1]][0]['points'])] = pair
        bff_by_score = sorted(bff_by_score.items(), key=lambda k: k, reverse=True)[:3]
        top_bff = [x[1] for x in bff_by_score]
        top_bff_json = {
            '1': [top_bff[0][0], top_bff[0][1]],
            '2': [top_bff[1][0], top_bff[1][1]],
            '3': [top_bff[2][0], top_bff[2][1]]
        }
        insert_to_db(client, top_bff_json, 'bff')
        print(top_bff)
    else:
        print(bff)


if __name__ == '__main__':
    workflow()
