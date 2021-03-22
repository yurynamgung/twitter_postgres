CREATE EXTENSION postgis;

\set ON_ERROR_STOP on

BEGIN;

CREATE TABLE urls (
    id_urls BIGSERIAL PRIMARY KEY,
    url TEXT UNIQUE
);

/*
 * Users may be partially hydrated with only a name/screen_name 
 * if they are first encountered during a quote/reply/mention 
 * inside of a tweet someone else's tweet.
 */
CREATE TABLE users (
    id_users BIGINT PRIMARY KEY,
    created_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ,
    id_urls BIGINT REFERENCES urls(id_urls),
    friends_count INTEGER,
    listed_count INTEGER,
    favourites_count INTEGER,
    statuses_count INTEGER,
    protected BOOLEAN,
    verified BOOLEAN,
    screen_name TEXT,
    name TEXT,
    location TEXT,
    description TEXT,
    withheld_in_countries VARCHAR(2)[],
    FOREIGN KEY (id_urls) REFERENCES urls(id_urls)
);

/*
 * Tweets may be entered in hydrated or unhydrated form.
 */
CREATE TABLE tweets (
    id_tweets BIGINT PRIMARY KEY,
    id_users BIGINT,
    created_at TIMESTAMPTZ,
    in_reply_to_status_id BIGINT,
    in_reply_to_user_id BIGINT,
    quoted_status_id BIGINT,
    retweet_count SMALLINT,
    favorite_count SMALLINT,
    quote_count SMALLINT,
    withheld_copyright BOOLEAN,
    withheld_in_countries VARCHAR(2)[],
    source TEXT,
    text TEXT,
    country_code VARCHAR(2),
    state_code VARCHAR(2),
    lang TEXT,
    place_name TEXT,
    geo geometry,
    FOREIGN KEY (id_users) REFERENCES users(id_users),
    FOREIGN KEY (in_reply_to_user_id) REFERENCES users(id_users)

    -- NOTE:
    -- We do not have the following foreign keys because they would require us
    -- to store many unhydrated tweets in this table.
    -- FOREIGN KEY (in_reply_to_status_id) REFERENCES tweets(id_tweets),
    -- FOREIGN KEY (quoted_status_id) REFERENCES tweets(id_tweets)
);
CREATE INDEX tweets_index_geo ON tweets USING gist(geo);
CREATE INDEX tweets_index_withheldincountries ON tweets USING gin(withheld_in_countries);

CREATE TABLE tweet_urls (
    id_tweets BIGINT,
    id_urls BIGINT,
    PRIMARY KEY (id_tweets, id_urls),
    FOREIGN KEY (id_tweets) REFERENCES tweets(id_tweets),
    FOREIGN KEY (id_urls) REFERENCES urls(id_urls)
);
--CREATE UNIQUE INDEX tweet_urls_unique ON tweet_urls(id_tweets,id_urls);


CREATE TABLE tweet_mentions (
    id_tweets BIGINT,
    id_users BIGINT,
    PRIMARY KEY (id_tweets, id_users),
    FOREIGN KEY (id_tweets) REFERENCES tweets(id_tweets),
    FOREIGN KEY (id_users) REFERENCES users(id_users)
);
CREATE INDEX tweet_mentions_index ON tweet_mentions(id_users);
--CREATE UNIQUE INDEX tweet_mentions_unique ON tweet_mentions(id_users,id_tweets);

CREATE TABLE tweet_tags (
    id_tweets BIGINT,
    tag TEXT,
    PRIMARY KEY (id_tweets, tag),
    FOREIGN KEY (id_tweets) REFERENCES tweets(id_tweets)
);
COMMENT ON TABLE tweet_tags IS 'This table links both hashtags and cashtags';
CREATE INDEX tweet_tags_index ON tweet_tags(id_tweets);
--CREATE UNIQUE INDEX tweet_tags_unique ON tweet_tags(tag,id_tweets);
--CREATE INDEX tweet_tags_unique2 ON tweet_tags(lower(tag),id_tweets);


CREATE TABLE tweet_media (
    id_tweets BIGINT,
    id_urls BIGINT,
    type TEXT,
    PRIMARY KEY (id_tweets, id_urls),
    FOREIGN KEY (id_urls) REFERENCES urls(id_urls),
    FOREIGN KEY (id_tweets) REFERENCES tweets(id_tweets)
);
--CREATE UNIQUE INDEX tweet_media_unique ON tweet_media(id_tweets,id_urls);

/*
 * Precomputes the total number of occurrences for each hashtag
 */
CREATE MATERIALIZED VIEW tweet_tags_total AS (
    SELECT 
        row_number() over (order by count(*) desc) AS row,
        tag, 
        count(*) AS total
    FROM tweet_tags
    GROUP BY tag
    ORDER BY total DESC
);

/*
 * Precomputes the number of hashtags that co-occur with each other
 */
CREATE MATERIALIZED VIEW tweet_tags_cooccurrence AS (
    SELECT 
        t1.tag AS tag1,
        t2.tag AS tag2,
        count(*) AS total
    FROM tweet_tags t1
    INNER JOIN tweet_tags t2 ON t1.id_tweets = t2.id_tweets
    GROUP BY t1.tag, t2.tag
    ORDER BY total DESC
);

/*
select table_name,constraint_name from information_schema.table_constraints where constraint_schema='public' and constraint_type='FOREIGN KEY';
   table_name   |         constraint_name         
----------------+---------------------------------
 users          | users_id_urls_fkey
 users          | users_id_urls_fkey1
 tweets         | tweets_id_users_fkey
 tweets         | tweets_in_reply_to_user_id_fkey
 tweet_urls     | tweet_urls_id_tweets_fkey
 tweet_urls     | tweet_urls_id_urls_fkey
 tweet_mentions | tweet_mentions_id_tweets_fkey
 tweet_mentions | tweet_mentions_id_users_fkey
 tweet_tags     | tweet_tags_id_tweets_fkey
 tweet_media    | tweet_media_id_urls_fkey
 tweet_media    | tweet_media_id_tweets_fkey
*/

ALTER TABLE users          DROP CONSTRAINT users_id_urls_fkey;
ALTER TABLE users          DROP CONSTRAINT users_id_urls_fkey1;
ALTER TABLE tweets         DROP CONSTRAINT tweets_id_users_fkey;
ALTER TABLE tweets         DROP CONSTRAINT tweets_in_reply_to_user_id_fkey;
ALTER TABLE tweet_urls     DROP CONSTRAINT tweet_urls_id_tweets_fkey;
ALTER TABLE tweet_urls     DROP CONSTRAINT tweet_urls_id_urls_fkey;
ALTER TABLE tweet_mentions DROP CONSTRAINT tweet_mentions_id_tweets_fkey;
ALTER TABLE tweet_mentions DROP CONSTRAINT tweet_mentions_id_users_fkey;
ALTER TABLE tweet_tags     DROP CONSTRAINT tweet_tags_id_tweets_fkey;
ALTER TABLE tweet_media    DROP CONSTRAINT tweet_media_id_urls_fkey;
ALTER TABLE tweet_media    DROP CONSTRAINT tweet_media_id_tweets_fkey;

COMMIT;
