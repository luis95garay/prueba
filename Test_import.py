import slack
import os
from pathlib import Path
from dotenv import load_dotenv
import pandas as pd
from datetime import date, datetime, timedelta
from google.cloud import storage

env_path = Path('.') / '.env'
load_dotenv(dotenv_path=env_path)
client = slack.WebClient(token=os.environ['SLACK_TOKEN'])
#print(client.users_info(user='U037XNZGRB7'))
channels = client.users_conversations(types='im', user='U03LYEN76MV')
print(channels)
channels = channels.get('channels', {})
for i in channels:
    print(i)

#U03LYEN76MV - D03MFEW8PGC', 'created': 1656369791
#U03M46EDVPY