import os
import time
import logging
from dataservice.body import Expressions, Body
from dataservice.query_configuration import QueryConfiguration
from dataservice.sdk import Client
import pandas as pd
import requests
import json
from google.cloud import firestore
from apscheduler.schedulers.background import BackgroundScheduler

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# data service authentication
DS_APPKEY = 'resbbi_general.dataservice.simml.5htCFNs8'
DS_APPSECRET = 'os1o8cu6i1fmo3cg'
DECRYPTION_KEY = 'D05B1yvZr3VGRwHIbAfTuYQHKBfum3Z2'

# firestore authentication
firestore_path = "react-simml-firebase-adminsdk-qrzgh-165d958a7c.json"
# Set the environment variable
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = firestore_path

# Initialize Firestore client
db = firestore.Client()

def init_client(appKey, appSecret):
    c = Client() \
    .create() \
    .env(QueryConfiguration.Env.EXTRANET) \
    .key(DECRYPTION_KEY)\
    .queryPattern(0) \
    .appKey(appKey) \
    .appSecret(appSecret) \
    .refresh()
    return c

def retrieve_simml_meta_registration_function():
    logger.info("Retrieving SIMML meta registration data...")
    # Initialize the Client
    c = init_client(DS_APPKEY, DS_APPSECRET)
    expressions = Expressions().getExpressions()
    # Prepare an empty request body
    body = Body(expressions).__dict__
    # Call the API and print results
    for i in c.call(api_abbr="resbbi_general.simml_meta_registration.dev", version="bjkcw2spawpx7cgl", body=body):
        raw_data = i
    # Transform data into a DataFrame
    df = pd.DataFrame([entry["values"] for entry in raw_data])
    logger.info("SIMML meta registration data retrieved.")
    return df

def get_asset_name(row):
    try:
        other_info_dict = json.loads(row['other_info'])
        asset_name = other_info_dict.get('name', '')
        if asset_name:
            return asset_name
    except (SyntaxError, ValueError):
        pass
    
    return f"{row['task_name']}_{row['asset_id']}"

def load_and_preprocess():
    logger.info("Loading and preprocessing data...")
    ori = retrieve_simml_meta_registration_function()
    ori['creation_datetime'] = pd.to_datetime(ori['creation_datetime'])
    f1 = ori['other_info'].apply(lambda x: json.loads(x)['status']!="init")
    f2 = ori['other_info'].apply(lambda x: json.loads(x)['status']!="error")
    ori_fil = ori[f1&f2]
    def parse_task_code(x):
        try:
            if "adhoc" in x:
                return x.replace("_" + x.split("_")[-1], "")
            return x.replace("_" + x.split("_")[-3] + "_" + x.split("_")[-2] + "_" + x.split("_")[-1], "")
        except:
            return x
        
    def form_url(x):
        if "adhoc" in x['instance_code']:
            return f"https://datasuite.shopee.io/scheduler/dev/adhoc/{x['instance_code']}/log"
        return f"https://datasuite.shopee.io/scheduler/dev/task/{x['task_code']}/instance/{x['instance_code']}/detail"
    df = ori_fil.loc[ori_fil.groupby('path')['creation_datetime'].idxmax()]
    df['last_updated_datetime'] = pd.to_datetime(df['last_updated_datetime'])
    df['task_id'] = df.apply(lambda x: x['project_name'] + "_" + x['name'], axis=True)
    df['asset_id'] = df['path'].apply(lambda x: x.split('/')[-2])
    print(df['instance_code'])
    df['task_code'] = df['instance_code'].apply(lambda x: parse_task_code(x))
    df['url'] = df.apply(lambda x:  form_url(x), axis=True)
    df['task_name'] = df.apply(lambda x: x['project_name'] + '/' + x['name'], axis=1)
    df['asset_name'] = df.apply(get_asset_name, axis=1)

    f2 = df['instance_code'].apply(lambda x: "instance_code" not in x.lower())

    df_fil = df[f2]
    logger.info("Data loaded and preprocessed.")
    return df_fil


def update_data_to_firestore(df):
    logger.info("Updating data to Firestore...")
    # Get unique task IDs from the DataFrame
    task_ids = df['task_id'].unique()
    c = 0
    s = 0
    # Iterate over each task ID
    for task_id in task_ids:
        logger.info(f"Updating task {task_id}...")
        # Get the subset of the DataFrame for the current task
        task_df = df[df['task_id'] == task_id]

        # Check if the task document exists
        task_ref = db.collection('tasks').document(str(task_id))
        task_doc = task_ref.get()
        # Update the task document
        task_data = {
            'asset_count': len(task_df),
            'creation_date': task_df['creation_datetime'].min(),
            'creator': task_df['creator'].iloc[0],
            'last_update_date': task_df['last_updated_datetime'].max(),
            'name': task_df['task_name'].iloc[0],
            'types': task_df['types'].iloc[0]
        }
        if task_doc.exists:
            task_ref.update(task_data)
        else:
            task_ref.set(task_data)

        # Iterate over each asset for the current task
        for _, asset_row in task_df.iterrows():
            asset_id = asset_row['asset_id']
            logger.info(f"Updating asset {asset_id}...")

            # Check if the asset document exists
            asset_ref = task_ref.collection('assets').document(str(asset_id))
            asset_doc = asset_ref.get()
            asset_data = {
                'instance_code': asset_row['instance_code'],
                'last_update_date': asset_row['last_updated_datetime'],
                'name': asset_row['asset_name'],
                'other_info': asset_row['other_info'],
                'path': asset_row['path'],
                'updater': asset_row['updater'],
                'url': asset_row['url']
            }
            asset_data['markdown_raw'] = str(asset_data)

            if asset_doc.exists:
                asset_ref.update(asset_data)
            else:
                asset_ref.set(asset_data)
    logger.info("Data updated to Firestore.")

def main():
    logger.info("Starting the main process...")
    df = load_and_preprocess()
    update_data_to_firestore(df)
    logger.info("Main process completed.")

if __name__ == "__main__":
    logger.info("Starting the script...")
    scheduler = BackgroundScheduler()
    scheduler.add_job(main, 'interval', minutes=60)
    scheduler.start()
    logger.info("Scheduler started. Running the main process every 5 minutes.")

    main()  # Run the main process immediately

    try:
        while True:
            time.sleep(60)
    except (KeyboardInterrupt, SystemExit):
        logger.info("Stopping the script...")
        scheduler.shutdown()
        logger.info("Script stopped.")