import logging
from EurovisionStat.winning_eurovision_2019 import song_winners, spotify_songs, votes


def _setup_logging():
    # create logger
    logger = logging.getLogger('EurovisionStat')
    logger.setLevel(logging.DEBUG)

    # create console handler and set level to debug
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)

    # create formatter
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    # add formatter to ch
    ch.setFormatter(formatter)

    # add ch to logger
    logger.addHandler(ch)


def main():
    _setup_logging()
    song_winners.workflow()
    spotify_songs.workflow()
    votes.workflow()


if __name__ == "__main__":
    main()
