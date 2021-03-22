#!/usr/bin/python3

# process command line args
import argparse
parser = argparse.ArgumentParser()
parser.add_argument('--db',required=True)
parser.add_argument('--inputs',nargs='+',required=True)
parser.add_argument('--print_every',type=int,default=1000)
args = parser.parse_args()

# imports
import sqlalchemy
import os
import datetime
import zipfile
import io
import json

# create database connection
engine = sqlalchemy.create_engine(args.db, connect_args={
    'application_name': 'load_tweets.py',
    })
connection = engine.connect()

################################################################################
# helper functions
################################################################################

def remove_nulls(s):
    '''
    Postgres doesn't support strings with the null character \x00 in them,
    but twitter does.
    This helper function replaces the null characters with an escaped version so that they can be loaded into postgres.
    Technically, this means the data in postgres won't be an exact match of the data in twitter,
    because we are not also escaping the escaped strings.
    The added complexity of a fully working escaping system doesn't seem worth the benefits since null characters appear so rarely in twitter text.
    '''
    if s is None:
        return None
    else:
        return s.replace('\x00','\\x00')


def get_id_urls(url):
    '''
    Given a url, returns the corresponding id in the urls table.
    If no row exists for the url, then one is inserted automatically.
    '''
    sql = sqlalchemy.sql.text('''
    insert into urls 
        (url)
        values
        (:url)
    on conflict do nothing
    returning id_urls
    ;
    ''')
    res = connection.execute(sql,{'url':url}).first()
    if res is None:
        sql = sqlalchemy.sql.text('''
        select id_urls 
        from urls
        where
            url=:url
        ''')
        res = connection.execute(sql,{'url':url}).first()
    id_urls = res[0]
    return id_urls


################################################################################
# main functions
################################################################################

def insert_tweet(connection,tweet):
    '''
    Inserts the tweet into the database.

    Args:
        connection: a sqlalchemy connection to the postgresql db
        tweet: a dictionary representing the json tweet object
    '''

    # skip tweet if it's already inserted
    sql=sqlalchemy.sql.text('''
    SELECT id_tweets 
    FROM tweets
    WHERE id_tweets = :id_tweets
    ''')
    res = connection.execute(sql,{
        'id_tweets':tweet['id'],
        })
    if res.first() is not None:
        return

    # insert tweet within a transaction;
    # this ensures that a tweet does not get "partially" loaded
    with connection.begin() as trans:

        ########################################
        # insert into the users table
        ########################################
        if tweet['user']['url'] is None:
            user_id_urls = None
        else:
            user_id_urls = get_id_urls(tweet['user']['url'])

        # create/update the user
        sql = sqlalchemy.sql.text('''
            ''')

        ########################################
        # insert into the tweets table
        ########################################

        try:
            geo_coords = tweet['geo']['coordinates']
            geo_coords = str(tweet['geo']['coordinates'][0]) + ' ' + str(tweet['geo']['coordinates'][1])
            geo_str = 'POINT'
        except TypeError:
            try:
                geo_coords = '('
                for i,poly in enumerate(tweet['place']['bounding_box']['coordinates']):
                    if i>0:
                        geo_coords+=','
                    geo_coords+='('
                    for j,point in enumerate(poly):
                        geo_coords+= str(point[0]) + ' ' + str(point[1]) + ','
                    geo_coords+= str(poly[0][0]) + ' ' + str(poly[0][1])
                    geo_coords+=')'
                geo_coords+=')'
                geo_str = 'MULTIPOLYGON'
            except KeyError:
                if tweet['user']['geo_enabled']:
                    geo_str = None
                    geo_coords = None

        try:
            text = tweet['extended_tweet']['full_text']
        except:
            text = tweet['text']

        try:
            country_code = tweet['place']['country_code'].lower()
        except TypeError:
            country_code = None

        if country_code == 'us':
            state_code = tweet['place']['full_name'].split(',')[-1].strip().lower()
            if len(state_code)>2:
                state_code = None
        else:
            state_code = None

        try:
            place_name = tweet['place']['full_name']
        except TypeError:
            place_name = None

        # NOTE:
        # The tweets table has the following foreign key:
        # > FOREIGN KEY (in_reply_to_user_id) REFERENCES users(id_users)
        #
        # This means that every "in_reply_to_user_id" field must reference a valid entry in the users table.
        # If the id is not in the users table, then you'll need to add it in an "unhydrated" form.
        if tweet.get('in_reply_to_user_id',None) is not None:
            sql=sqlalchemy.sql.text('''
                ''')

        # insert the tweet
        sql=sqlalchemy.sql.text(f'''
            ''')

        ########################################
        # insert into the tweet_urls table
        ########################################

        try:
            urls = tweet['extended_tweet']['entities']['urls']
        except KeyError:
            urls = tweet['entities']['urls']

        for url in urls:
            id_urls = get_id_urls(url['expanded_url'])

            sql=sqlalchemy.sql.text('''
                ''')

        ########################################
        # insert into the tweet_mentions table
        ########################################

        try:
            mentions = tweet['extended_tweet']['entities']['user_mentions']
        except KeyError:
            mentions = tweet['entities']['user_mentions']

        for mention in mentions:
            # insert into users table;
            # note that we already have done an insert into the users table above for the user who sent a tweet;
            # that insert had lots of information inside of it (i.e. the user row was "hydrated");
            # when we only have a mention of a user, however, we do not have all the information to store in the row;
            # therefore, we must store the user info "unhydrated"
            # HINT:
            # use the ON CONFLICT DO NOTHING syntax
            sql=sqlalchemy.sql.text('''
                ''')

            # insert into tweet_mentions
            sql=sqlalchemy.sql.text('''
                ''')

        ########################################
        # insert into the tweet_tags table
        ########################################

        try:
            hashtags = tweet['extended_tweet']['entities']['hashtags'] 
            cashtags = tweet['extended_tweet']['entities']['symbols'] 
        except KeyError:
            hashtags = tweet['entities']['hashtags']
            cashtags = tweet['entities']['symbols']

        tags = [ '#'+hashtag['text'] for hashtag in hashtags ] + [ '$'+cashtag['text'] for cashtag in cashtags ]

        for tag in tags:
            sql=sqlalchemy.sql.text('''
                ''')

        ########################################
        # insert into the tweet_media table
        ########################################

        try:
            media = tweet['extended_tweet']['extended_entities']['media']
        except KeyError:
            try:
                media = tweet['extended_entities']['media']
            except KeyError:
                media = []

        for medium in media:
            id_urls = get_id_urls(medium['media_url'])
            sql=sqlalchemy.sql.text('''
                ''')


# loop through the input file
# NOTE:
# we reverse sort the filenames because this results in fewer updates to the users table,
# which prevents excessive dead tuples and autovacuums
for filename in sorted(args.inputs, reverse=True):
    with zipfile.ZipFile(filename, 'r') as archive: 
        print(datetime.datetime.now(),filename)
        for subfilename in sorted(archive.namelist(), reverse=True):
            with io.TextIOWrapper(archive.open(subfilename)) as f:
                for i,line in enumerate(f):

                    # load and insert the tweet
                    tweet = json.loads(line)
                    insert_tweet(connection,tweet)

                    # print message
                    if i%args.print_every==0:
                        print(datetime.datetime.now(),filename,subfilename,'i=',i,'id=',tweet['id'])
