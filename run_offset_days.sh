#!/bin/bash

BEG=23
END=23
SIZE=1

for i in `seq $BEG $SIZE $END`;
do
	echo "sed "s/days=1/days=$i/g" tweet_json_to_csv_to_tar_cron.py > offset_days_tweet_json_to_csv_to_tar_cron.py"
	sed "s/days=1/days=$i/g" tweet_json_to_csv_to_tar_cron.py > offset_days_tweet_json_to_csv_to_tar_cron.py
	python3 offset_days_tweet_json_to_csv_to_tar_cron.py
	#echo $i
done

echo "ALL DONE"
