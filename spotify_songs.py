import json
import logging
import spotipy
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
    genres = []
    for artist in artists:
        artist_data = sp.artist(artist["id"])
        ganre = artist_data['genres']
        genres.append(ganre)
    return genres


def parse_songs(all_songs):
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
    print(json.dumps(data, indent=4, sort_keys=True))


def get_song_key(song):
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


def workflow():
    LOGGER.info(f"Start downloading songs from spotify.")
    playlist = sp.user_playlist_tracks(
        user=spotify_config.EUROVISION_PLYLIST_USER,
        playlist_id=spotify_config.EUROVISION_PLAYLIST_ID
    )
    songs = parse_songs(playlist['items'])
    insert_to_db(mongo_client, songs, 'winners_songs_spotify')

