#!/bin/sh
# example to run this script
# nohup ./run.sh input_04_01-02 output_04_01-02 > run0401.out 2>&1 < run0401.out &

INPUT_DIR=$1
OUTPUT_DIR=$2
#INPUT_DIR=/work/03076/rhuang/maverick2/covid_tweets/input_dir
#OUTPUT_DIR=/work/03076/rhuang/maverick2/covid_tweets/output_dir

for filename in $INPUT_DIR/*; do
	echo python3 tweet_json_to_csv.py $filename $OUTPUT_DIR
	python3 tweet_json_to_csv.py $filename $OUTPUT_DIR
done

echo 'ALL DONE'
