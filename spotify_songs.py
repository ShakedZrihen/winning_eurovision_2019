import json
import logging

import requests
import spotipy
from bs4 import BeautifulSoup
from pymongo import MongoClient
from spotipy.oauth2 import SpotifyClientCredentials
from EurovisionStat.winning_eurovision_2019.config import spotify as spotify_config, db

LOGGER = logging.getLogger(__name__)

client_credentials_manager = SpotifyClientCredentials(
        client_id=spotify_config.CLIENT_ID,
        client_secret=spotify_config.CLIENT_SECRET
    )
sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)

mongo_client = MongoClient(f'mongodb://{db.USERNAME}:{db.PASSWORD}@{db.HOST}:{db.PORT}/{db.NAMESPACE}')


def get_genres(artists):
    # type: (list)->list
    """
    Get genres of artist from Spotify API
    :param artists: A list of artists
    :return: A list of artists genres
    """
    genres = []
    for artist in artists:
        artist_data = sp.artist(artist["id"])
        genre = artist_data['genres']
        genres.append(genre)
    return genres


def parse_songs(all_songs):
    # type: (list) -> list
    """
    Get all eurovision songs from spotify API and takes only wanted parameters
    :param all_songs: List of songs from spotify API
    :return: List of parsed songs
    """
    LOGGER.info(f"Get all songs from playlist")
    song_list = []
    for song in all_songs:
        track = song['track']
        new_song = {
            'name': track['name'],
            'id': track['id'],
            'artist': [artist['name'] for artist in track['artists']],
            'date': track['album']['release_date'],
            'key': get_song_key(track),
            'genres': get_genres(track['artists'])
        }
        song_list.append(new_song)
    return song_list


def print_all_data(data):
    """
    Get data as dict or list object and print it as indented JSON
    :param data: Dict or List object
    :return: None
    """
    print(json.dumps(data, indent=4, sort_keys=True))


def get_song_key(song):
    """
    Convert song's key represented by number to key name
    :param song: Song object from Spotify API
    :return: Song key represented as string
    """
    LOGGER.info(f"Get song key. song: {song['name']}")
    analysis = sp.audio_analysis(song['id'])
    music_keys = {
        '0': 'C',
        '1': 'C#',
        '2': 'D',
        '3': 'D#',
        '4': 'E',
        '5': 'F',
        '6': 'F#',
        '7': 'G',
        '8': 'G#',
        '9': 'A',
        '10': 'A#',
        '11': 'B'
    }
    music_key = str(analysis['track']['key'])
    return music_keys[music_key]


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
    LOGGER.info(f"Insert document to collection: {collection_name}")
    if type(documents) is dict:
        documents = [documents]
    if type(documents) is list:
        for doc in documents:
            collection = eurovision_db[collection_name]
            collection.insert_one(doc)
    else:
        raise TypeError


def get_song_number_in_final():
    """
    Get all winner's song order place number in final
    :return: dict: key: song name, value: song order place in final
    """
    url = 'https://eschome.net/databaseoutput202.php'
    headers = {'content-type': 'application/x-www-form-urlencoded'}
    result = requests.post(url, headers=headers)
    bs = BeautifulSoup(result.text, 'html.parser')
    table = bs.find("table", {"id": "tabelle1"})
    rows = table.find_all('tr')
    songs_numbers = {}
    for row in rows[1:]:
        _song_name = row.find_all('td')[6]
        song_number = row.find_all('td')[2]
        songs_numbers[_song_name.text.replace('.', '').replace(',', '')] = song_number.text
    insert_to_db(mongo_client, songs_numbers, 'winner_songs_perform_number')
    return songs_numbers


def merge_collections():
    """
    Merge winner by year collection with spotify song data and update it in db
    :return: None
    """
    eurovision_db = mongo_client.eurovision
    winner_by_year_collection = eurovision_db['winners_by_year'].find_one()
    for key in winner_by_year_collection:
        try:
            song_name = winner_by_year_collection[key]['song']
            spotify_songs = eurovision_db['winners_songs_spotify'].find()
            for doc in spotify_songs:
                if doc['name'] in song_name:
                    winner_by_year_collection[key]['song'] = doc
                    insert_to_db(mongo_client, winner_by_year_collection[key], 'winners_by_year')
                    break
        except Exception as e:
            pass


def extract_winner_from_country():
    """
    Build winner by location from existing "winners by year" collection
    :return: dict with all winners by location of the competition
    """
    eurovision_db = mongo_client.eurovision
    all_winning_by_location = {}
    winner_by_year_collection = eurovision_db['winners_by_year'].find()
    for doc in winner_by_year_collection:
        location = ''
        try:
            location = doc['host_city'].replace(' ', '').lower()
            doc.pop('_id', None)
            doc["song"].pop('_id', None)
            all_winning_by_location[location].append(doc)
        except KeyError:
            all_winning_by_location[location] = [doc]
    insert_to_db(mongo_client, all_winning_by_location, 'all_winners_by_location')
    return all_winning_by_location


def workflow():
    LOGGER.info(f"Start downloading songs from spotify.")
    playlist = sp.user_playlist_tracks(
        user=spotify_config.EUROVISION_PLYLIST_USER,
        playlist_id=spotify_config.EUROVISION_PLAYLIST_ID
    )
    songs = parse_songs(playlist['items'])
    return songs

