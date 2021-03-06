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
    """
    Setup global logger for logging progress
    Run all workflows:
    - get all song winners and their counties,
    - bind song name to song data from spotify and get votes
    - get votes from/to country from all over the years
    :return: None
    """
    _setup_logging()
    song_winners.workflow()
    spotify_songs.workflow()
    votes.workflow()


if __name__ == "__main__":
    main()
