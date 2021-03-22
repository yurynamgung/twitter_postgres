files='
test-data.zip
'

for file in $files; do
    # call the load_tweets.py file to load data into pg_normalized
done

for file in $files; do
    # use SQL's COPY command to load data into pg_denormalized
done
