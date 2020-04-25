from tweet_json_to_csv_to_tar_cron import yesterday_filename_prefix
import os, glob, csv
from datetime import date
from datetime import datetime



output_dir = '/corral-repl/data/COVID-19-tweets/tweet_id'
output_file_name = output_dir + '/' + 'summary.csv'
print(output_file_name)

header = ['date', 'rows_all', 'size_all_JSON_MB', 'rows_original', 'size_original_csv_MB']


# begin = '2020-03-25'
# begin_date = datetime.strptime(begin, "%Y-%m-%d").date()
# print(begin_date)
# today = date.today()
# print(today)
# date_diff = abs((today - begin_date).days)
# print(date_diff) 

for offset in range(1, 0, -1):

    offset= offset

    dir_all_tweet_id = '/corral-repl/data/COVID-19-tweets/tweet_id/all_tweet_id/'
    dir_org_tweet_id = '/corral-repl/data/COVID-19-tweets/tweet_id/original_tweet_id/'
    dir_log = '/corral-repl/data/COVID-19-tweets/log' 
    dir_csv = '/corral-repl/data/COVID-19-tweets/log_csv'

    date = yesterday_filename_prefix('', offset)

    tweet_id_file_name = yesterday_filename_prefix(fixed_str='tweet-id-',offset_days=offset) + '.txt'
    all_tweet_id_file_name = dir_all_tweet_id + '/' + tweet_id_file_name
    org_tweet_id_file_name = dir_org_tweet_id + '/' + tweet_id_file_name
    json_files_name = dir_log + '/' + yesterday_filename_prefix('',offset) + '/' + \
                    yesterday_filename_prefix('tweets.log.', offset) + '*'
    csv_files_name = dir_csv + '/' + yesterday_filename_prefix('',offset) + '/' + \
                    yesterday_filename_prefix('tweets.log.', offset) + '*.csv'

    print(all_tweet_id_file_name)
    print(org_tweet_id_file_name)
    print(json_files_name)
    print(csv_files_name)
    file_list_json = glob.glob(json_files_name)
    file_list_csv = glob.glob(csv_files_name)
    file_list_json.sort()
    file_list_csv.sort()
    len(file_list_csv)

    size_all_JSON_MB = round(sum(os.stat(f).st_size for f in file_list_json)/(1024**2),1)
    size_original_csv_MB = round(sum(os.stat(f).st_size for f in file_list_csv)/(1024**2),1)

    rows_all = sum(1 for line in open(all_tweet_id_file_name, 'r'))
    rows_original = sum(1 for line in open(org_tweet_id_file_name, 'r'))


    summary = [date, rows_all, size_all_JSON_MB, rows_original, size_original_csv_MB]


    if not os.path.isfile(output_file_name):
        with open(output_file_name, 'a') as f:
            csv_writer = csv.writer(f)
            print('append %s' % header)
            csv_writer.writerow(header)

    with open(output_file_name, 'a') as f:
        csv_writer = csv.writer(f)
        print('append %s' % summary)
        csv_writer.writerow(summary)


