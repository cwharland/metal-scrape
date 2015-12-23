# metal-archive json data scraper
from bs4 import BeautifulSoup as bs
import pandas as pd
import urllib2 as urllib
import json
import pyprind
import re
import random
import time


# Global configs
MAX_RECORD_FIELD = 'iTotalRecords'
DATA_FIELD = 'aaData'
STEP = 200
RATE_LIMIT = 1  # number of seconds per request (google crawler is 1s/req)
USER_AGENT_LIST = ["Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/22.0.1207.1 Safari/537.1",
                   "Mozilla/5.0 (X11; CrOS i686 2268.111.0) AppleWebKit/536.11 (KHTML, like Gecko) Chrome/20.0.1132.57 Safari/536.11",
                   "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.6 (KHTML, like Gecko) Chrome/20.0.1092.0 Safari/536.6",
                   "Mozilla/5.0 (Windows NT 6.2) AppleWebKit/536.6 (KHTML, like Gecko) Chrome/20.0.1090.0 Safari/536.6",
                   "Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/19.77.34.5 Safari/537.1",
                   "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/536.5 (KHTML, like Gecko) Chrome/19.0.1084.9 Safari/536.5",
                   "Mozilla/5.0 (Windows NT 6.0) AppleWebKit/536.5 (KHTML, like Gecko) Chrome/19.0.1084.36 Safari/536.5",
                   "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1063.0 Safari/536.3",
                   "Mozilla/5.0 (Windows NT 5.1) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1063.0 Safari/536.3",
                   "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_8_0) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1063.0 Safari/536.3",
                   "Mozilla/5.0 (Windows NT 6.2) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1062.0 Safari/536.3",
                   "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1062.0 Safari/536.3",
                   "Mozilla/5.0 (Windows NT 6.2) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1061.1 Safari/536.3",
                   "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1061.1 Safari/536.3",
                   "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1061.1 Safari/536.3",
                   "Mozilla/5.0 (Windows NT 6.2) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1061.0 Safari/536.3",
                   "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/535.24 (KHTML, like Gecko) Chrome/19.0.1055.1 Safari/535.24",
                   "Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/535.24 (KHTML, like Gecko) Chrome/19.0.1055.1 Safari/535.24"]


def _read_json(url):
    '''
    Reads and parses json result from url
    '''
    req = urllib.Request(url)
    req.add_header('User-agent', random.choice(USER_AGENT_LIST))  # avoid rate limit
    r = urllib.urlopen(req)
    return json.load(r)


def _get_link_text(html):
    '''
    Gets the text of a single html link
    '''
    soup = bs(html)
    return soup.find('a').string


def _get_contents(html):
    '''
    Generic contents wrapper
    '''
    soup = bs(html)
    return soup.string


def _get_lyric_id(html):
    '''
    pulls lyric_id from a tag
    '''
    soup = bs(html)
    nums = re.findall('\d+', soup.find('a')['id'])
    return int(nums[0])


def _clean_str(input_str):
    '''
    Remove tabs, new line, carriage return, crazy spaces
    '''
    if not isinstance(input_str, basestring):
        return input_str
    else:
        space_killer = re.compile("\s+")
        return space_killer.sub(' ', input_str).strip()


def _parse_artist_genre(data):
    '''
    Pulls out artist, genre, country from json scrape
    '''
    artist = _get_link_text(data[0])
    output = [artist, data[1], data[2]]
    return [_clean_str(x) for x in output]


def _parse_song(data):
    '''
    Pulls out band, album, type, song title, lyric id
    '''
    band = _get_contents(data[0])
    album = _get_link_text(data[1])
    album_type = data[2]
    song_title = data[3]
    lyric_id = _get_lyric_id(data[4])
    output = [band, album, album_type, song_title, lyric_id]
    return [_clean_str(x) for x in output]


def _pull_json_data(request_url,
                    max_record_field=MAX_RECORD_FIELD,
                    data_field=DATA_FIELD,
                    step=STEP,
                    sleep=RATE_LIMIT,
                    record_limit=None):
    '''
    Pulls json data from given request_url
    '''
    all_data = []
    if record_limit is not None:
        record_range = range(0, record_limit, step)
    else:
        # Find total number of records and grab first batch
        data = _read_json(request_url.format(0, step))
        max_records = data[max_record_field]
        all_data.extend(data[data_field])
        # be nice rate limit
        time.sleep(RATE_LIMIT)
        record_range = range(step, max_records, step)

    # iterate over all pages and get artists and genres
    for i in record_range:
        data = _read_json(request_url.format(i, step))
        all_data.extend(data[data_field])
        time.sleep(RATE_LIMIT)

    return all_data


def _get_lyrics(lyric_id):
    '''
    Retrieves lyrics for given id dealing with missing lyrics
    '''
    request_url = 'http://www.metal-archives.com/release/ajax-view-lyrics/id/{0:d}'
    data = urllib.urlopen(request_url.format(lyric_id))
    soup = bs(data)
    lyrics = _clean_str(soup.text)
    if lyrics == '(lyrics not available)':
        lyrics = None
    return lyrics


def get_artist_genre_table(record_limit=None):
    '''
    Helper to download artists and genres from metal-archives
    '''
    ajax_request_url = 'http://www.metal-archives.com/search/ajax-advanced/searching/bands?iDisplayStart={0:d}&iDisplayLength={1:d}'
    all_data = _pull_json_data(ajax_request_url, record_limit=record_limit)

    # Parse each request to extract artist, genre, country
    col_names = ['artist', 'genre', 'country']
    all_data = [_parse_artist_genre(x) for x in all_data]

    # Format into dataframe
    df = pd.DataFrame(all_data, columns=col_names)

    return df


def get_song_table(record_limit=None):
    '''
    Pulls down all song data
    '''
    ajax_request_url = 'http://www.metal-archives.com/search/ajax-advanced/searching/songs?iDisplayStart={0:d}&iDisplayLength={1:d}'
    all_data = _pull_json_data(ajax_request_url, record_limit=record_limit)

    # Parse and dataframe it
    col_names = ['artist', 'album', 'album_type', 'song', 'lyric_id']
    all_data = [_parse_song(x) for x in all_data]
    df = pd.DataFrame(all_data, columns=col_names)

    return df
