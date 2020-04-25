# Import smtplib for the actual sending function
import smtplib
import glob
from datetime import datetime
import time, os


# Import the email modules we'll need
from email.message import EmailMessage


def checkGeneratedFiles(path = '/corral-repl/data/COVID-19-tweets/log', prefix = 'tweets.log.'):
    path = '/corral-repl/data/COVID-19-tweets/log'
    prefix = 'tweets.log.'
    dt_string = datetime.now().strftime("%Y-%m-%d-%H-%M")
    #dt_string = '2020-04-24-15-00'
    file_name = path + '/' + prefix + dt_string

    print(file_name)
    time.sleep(1920)

    if glob.glob(file_name):
        file_list = glob.glob(path + '/' + '*' +dt_string)
        file_list.sort()
        size = [str(round(os.stat(f).st_size/(1024**2),1)) for f in file_list]
        res = ['\t'.join(t) for t in list(zip(file_list, size))]
        multi_line_string = '\n'.join(res)
        return multi_line_string
    

msg = EmailMessage()
msg.set_content(checkGeneratedFiles())

print('email sent')


# me == the sender's email address
# you == the recipient's email address
msg['Subject'] = 'tweets collection run on idols VM'
msg['From'] = 'rhuang@tacc.utexas.edu'
msg['To'] = 'rhuang@tacc.utexas.edu'

# Send the message via our own SMTP server.
s = smtplib.SMTP('localhost')
s.send_message(msg)
s.quit()


