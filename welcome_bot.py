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
#client.chat_postMessage(channel="U037XNZGRB7", text=f"Hello <@U037XNZGRB7>!!!, Welcome to Digital Transformation Team!. We are so happy to have you here!. You have come to a wonderful team and with you we can be sure that we are going to be better! :rocket:  :medal: \n\nI want to introduce myself. I am Orion Bot :robot_face: and I am here in order tell you that one of the projects that the team is working on is called ”Orion” with the main objetive of develop a relational analytics of the team. In order to achive that, I need to record all the messages the team is writing in public channels, but don't worry, only in public channels, don't be afraid of your personal chats. It is also important to say that we are not reading the messages, we are only using the interactions between users (reactions, replies, mentions). Any doubt that you have, you can ask to any member of Orion project. \n\nOnce again we want to welcome you and wish you have a good time in Applaudo with these wonderful team. :tada: :raised_hands:")

def _get_blob_gcp_signed_url()->str:
    '''
    Get signed url to access blob storage
    '''
    storage_client = storage.Client.from_service_account_json('credentials.json')
    file = "orion_slack/users_info/previous_users.parquet"
    expiration_time = datetime.utcnow() + timedelta(minutes=2)
    bucket = storage_client.get_bucket("test-raw-repository")
    blob = storage.Blob(file, bucket)
    storage_client.close()
    return blob.generate_signed_url(expiration=expiration_time)

def obtain_previous_users()->pd.DataFrame:
    """
    Read data from blob storage and process it to a pandas dataframe
    Parameters:
    ----------
    None

    returns:
    ----------
    previous_users: pd.DataFrame
    """
    #previous_users = pd.read_parquet("previous_users.parquet",engine="pyarrow")

    url = _get_blob_gcp_signed_url()
    previous_users = pd.read_parquet(url)
    return previous_users


def obtain_current_users()->pd.DataFrame:
    """
    This function obtain the current users from a workspace through slack api
    Parameters:
    ----------
    None

    returns:
    ----------
    current_users: pd.DataFrame
    """
    payload = client.users_list()
    members = payload.get('members', {})
    current_users = {f"{member['id']}": [member['name'], date.today()] for member in members
                     if (member['deleted'] is False and member['is_email_confirmed'] is True)}
    current_users = pd.DataFrame.from_dict(current_users, columns=['name', 'last_date'],
                                           orient="index").reset_index().rename(columns={"index": "id"})
    return current_users


def obtain_new_users(current_users:pd.DataFrame,previous_users:pd.DataFrame)->pd.DataFrame:
    """
    This function match the previous and current tables from users in order to obtain the new users
    Parameters:
    ----------
    previous_users: pd.DataFrame, the users reported in the last run
    current_users: pd.DataFrame, the users reported in the current run

    returns:
    ----------
    match_users: pd.DataFrame, new users
    """
    match_users = pd.merge(current_users, previous_users[['id','name']], on='id', how='left')
    new_users = match_users[match_users['name_y'].isna()]
    return new_users



def send_message(new_users:pd.DataFrame):
    """
    This function send a direct welcome message to the new users
    Parameters:
    ----------
    match_users: pd.DataFrame, new users

    returns:
    ----------
    None
    """
    for user_id in new_users['id'].unique().tolist():
        text_welcome = f"Hello <@{user_id}>!!!, Welcome to Digital Transformation Team!. We are so happy to have you here!. You have come to a wonderful team and with you, we can be sure that we are going to be better! :rocket:  :medal: \n\nI want to introduce myself. I am Orion Bot :robot_face: and I am here to tell you that one of the projects that the team is working on is called ”Orion” with the main objective of developing relational analytics for the team. In order to achieve that, I need to record all the messages the team is writing on public channels, but don't worry, only on public channels, don't be afraid of your personal chats. It is also important to say that we are not reading the messages, we are only using the interactions between users (reactions, replies, mentions). Any doubt that you have, you can ask any member of the Orion project. \n\nOnce again, we want to welcome you and wish you to have a good time in Applaudo with this wonderful team. :tada: :raised_hands:"
        #print(f"Message sent to: <@{user_id}>")
        client.chat_postMessage(channel=user_id, text=text_welcome)
    print("Done!")


def update_users_table(current_users:pd.DataFrame):
    """
    This function updates de users table
    Parameters:
    ----------
    current_users: pd.DataFrame, users from current run

    returns:
    ----------
    None
    """
    current_users.to_parquet('previous_users.parquet',engine='pyarrow', index=False)
    storage_client = storage.Client.from_service_account_json('credentials.json')
    bucket = storage_client.get_bucket("test-raw-repository")
    blob = bucket.blob("orion_slack/users_info/previous_users.parquet")
    path_to_file = "previous_users.parquet"
    blob.upload_from_filename(path_to_file)

def main():
    previous_users = obtain_previous_users()
    print("previous users:\n", previous_users)
    print()
    current_users = obtain_current_users()
    print("Current users:\n", current_users)
    print()
    new_users = obtain_new_users(current_users,previous_users)
    print("New users:\n", new_users)
    print()
    send_message(new_users)
    update_users_table(current_users)


if __name__ == "__main__":
    main()


