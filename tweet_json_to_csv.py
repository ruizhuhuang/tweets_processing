import sys,os
import json
import csv
from datetime import datetime
from bs4 import BeautifulSoup
from os import listdir
from os.path import isfile, join

#  'Thu Mar 26 14:29:57 +0000 2020' -> 2020-03-26 14:29:57
def created_time(created_at:str):
    datetime_str = created_at[4:19] + created_at[-5:]
    datetime_object = datetime.strptime(datetime_str,'%b %d %X %Y')
    return datetime_object

def process_tweet_json_to_csv(input_file_path, output_dir_path):
    input_dir, input_file_name = os.path.split(input_file_path.strip())
    output_file_path = output_dir_path + '/' + input_file_name + '.csv'
    stat_file_path = output_dir_path + '/' + input_file_name + '.stat'
    
    #print(input_file_name)
    #print(output_file_path)
    msg_file_processing = 'processing %s ......' % input_file_path

    if os.path.exists(input_file_path):
        print(msg_file_processing)
        with open(output_file_path, 'w+', newline='') as csvfile:
            csv_writer = csv.writer(csvfile)

            csv_header = ['id', 'guid', 'link', 'pubdate', 'author', 'title', 'description', 'source', 'user_id', 
                      'profile_image_url', 'user_statuses_count', 'user_friends_count', 'user_followers_count',
                      'user_created_at', 'user_bio', 'user_location','lang', 'coords']
            csv_writer.writerow(csv_header)

            with open(input_file_path,'r') as f:
                i = 0
                line_num = 1
                error_row = 0
                for line in f:
                    if line_num >= 0:
                        line_object = json.loads(line)
                        retweeted_status = line_object.get("retweeted_status")

                        # if retweeted_status is None, then is not retweets
                        if not retweeted_status:                    
                            user_object = line_object.get("user", "unknown")
                            if user_object != 'unknown':
                                id = i + 1
                                #print(id)
                                user_object = line_object["user"]
                                screen_name = user_object.get("screen_name", "unknown")
                                id_str = line_object.get("id_str")
                                guid = 'https://twitter.com/%s/statuses/%s' % (screen_name, id_str)
                                link = guid
                                pubdate = line_object.get("created_at", "unknown")
                                if pubdate != 'unknown':
                                    pubdate = created_time(pubdate)
                                author = screen_name
                                text = line_object.get('text',"unknown")
                                title = text
                                description = text
                                source = line_object.get('source',"unknown")
                                if source != 'unknown':
                                    soup = BeautifulSoup(source,"html.parser")
                                    tag=soup.a
                                    if tag:
                                        source = tag.string
                                    else:
                                        source = "unknown"
                                user_id = user_object.get('id_str', 'unknown')
                                profile_image_url = user_object.get('profile_image_url_https', 'unknown')
                                user_statuses_count = user_object.get('statuses_count', 'unknown')
                                user_friends_count = user_object.get('friends_count', 'unknown')
                                user_followers_count = user_object.get('followers_count', 'unknown')
                                user_created_at = user_object.get('created_at', 'unknown')
                                if user_created_at != 'unknown':
                                    user_created_at = created_time(user_created_at)
                                user_bio = user_object.get('description', 'unknown')
                                user_location = user_object.get('location', 'unknown')
                                lang = line_object.get('lang', 'unknown')
                                coords = line_object.get('coordinates', 'unknown')
                                if coords:
                                    coords = coords.get('coordinates')
                                    coords = str(coords[0]) + ',' + str(coords[1])
                                one_row = [id,guid,link,pubdate,author,title,description,source,
                                           user_id,profile_image_url,user_statuses_count,user_friends_count,
                                           user_followers_count,user_created_at,user_bio,user_location,lang, coords]
                                csv_writer.writerow(one_row)
                                i += 1
                                #print('\n')
                            else:
                                #print(line)
                                error_row += 1
                                #print('error_row = %i' % error_row)
                        #print('processing row %i' % line_num)
                        line_num += 1

        #print('\n')
        msg_intput_rows = 'total rows processs: %i' % line_num
        msg_output_rows = 'total tweets(not retweets) in csv: %i' % i
        msg_error_rows  = 'total rows of rate limit json object: %i' % error_row
        print(msg_intput_rows)
        print(msg_output_rows)
        print(msg_error_rows)
        print('\n')
        with open(stat_file_path, 'w') as stat:
            stat.write(msg_file_processing +'\n')
            stat.write(msg_intput_rows +'\n')
            stat.write(msg_output_rows + '\n')
            stat.write(msg_error_rows + '\n')
    else:
        print('input file %s does not exit' % input_file_path)

def main():
    #input_path = 'tweets.log.2020-04-06-08-30'
    #output_dir = '/work/03076/rhuang/maverick2/covid_tweets/tweets_csv'
    input_path = sys.argv[1]
    output_dir = sys.argv[2]
    process_tweet_json_to_csv(input_path, output_dir)

if __name__=="__main__":
    main()


