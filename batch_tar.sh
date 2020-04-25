#!/bin/bash 

# example to run the script
# 1. mkdir input_04_06-09
# 2. change the dir in TAR below
# 3. source batch_copy.sh 2020-04 6 9

TAR="/gpfs/corral3/repl/tacc/dmc/rhuang/COVID-19-tweets/log_csv_daily"
CURDIR=`pwd`

#base source, date  and target location, 
SRC="/gpfs/corral3/repl/tacc/dmc/rhuang/COVID-19-tweets/log_csv/"
LOC="tweets.log"
YM=$1 #"2020-04"
#TAR="/work/03076/rhuang/maverick2/covid_tweets/input_04_06-09"

#dates to be copied.
BEG=$2  # 1
END=$3  # 10
SIZE=1


mkdir -p $TAR
for i in `seq $BEG $SIZE $END`; 
do 
   #_END=$(( $i + $SIZE - 1))
   _NAME="${LOC}.${YM}-$i"
   if [ $i -lt 10 ]; then _NAME="${LOC}.${YM}-0$i"; fi
   #echo "$_NAME";
   echo "cd $SRC && tar -cvzf $TAR/$_NAME.tgz $_NAME-*.csv && cd $CURDIR";
   cd $SRC && tar -cvzf $TAR/$_NAME.tgz $_NAME-*.csv && cd $CURDIR
done 


echo "All Done"

