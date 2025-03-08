from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from more_itertools.more import first
from sqlalchemy import false

#Retrieve data from API
#This API will be replaced after by real source data :IoT End devices
def getData() :
    import requests
    resp = requests.get('https://randomuser.me/api/')
    resp = resp.json()
    resp = resp['results'][0]
    return resp
#Extract infos I need in json data
def formatData(resp) :
    data = {}
    location = resp['location']
    data['first_name'] = resp ['name']['first']
    data['last_name'] = resp ['name']['last']
    data['gender'] = resp['gender']
    data['email'] = resp['email']
    data['address'] = (f"{str(location['street']['number'])} {str(location['street']['name'])} "
                       f"{str(location['city'])} {str(location['state'])}, {str(location['country'])}")
    data['postcode'] = location['postcode']
    data['username'] = resp['login']['username']
    """"
    data['password'] = resp['login']['password']
    data['uuid'] = resp['login']['uuid']
    data['salt'] = resp['login']['salt']
    data['md5'] = resp['login']['md5']
    data['sha1'] = resp['login']['sha1']
    data['sha256'] = resp['login']['sha256']
    """
    data['phone'] = resp['phone']
    data['picture'] = resp['picture']['large']

    return data
def streamDataFromAPI() :
    import json
    resp = getData()
    resp = formatData(resp)
    print(json.dumps(resp, indent=2))
default_args = {
    'owner' : 'korota-Team',
    'start_date' : datetime(2025, 3, 8, 0, 00),
    'retries' : 1
}
dag = DAG('ENSAM_LCCPS_Labo_automation',
          default_args = default_args,
          schedule = '@daily',
          catchup = False
          )
streamingTask = PythonOperator(
    task_id='streamDataFromAPI',
    python_callable=streamDataFromAPI,
    dag=dag
    )

streamDataFromAPI()