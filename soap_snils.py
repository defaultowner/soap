import requests
import uuid
import re
import datetime
import json
import pytz
import psycopg2
from dadata import Dadata
import time
import csv
import pandas as pd
import psycopg2 as pg
import pandas.io.sql as psql
from sqlalchemy import create_engine
import local_settings as settings

i = 0
connection1 = pg.connect(
    host="10.50.10.33",
    user=settings.USER,
    password=settings.PASSWORD,
    dbname="go-keeper"
)
dataframe_etl = psql.read_sql("select * from suoo_smev_snils limit 10 ", connection1)
connection1.close()

dataframe_etl.drop(['deleted_at', 'updated_at', 'created_at', 'job_id', 'area', 'exchange_job_id', 'id'], axis= 1 , inplace= True )


dataframe_etl['date_birth'] = pd.to_datetime(dataframe_etl['date_birth'])
dataframe_etl['date_birth'] = dataframe_etl['date_birth'].dt.strftime('%Y-%m-%d')
print(dataframe_etl)

df = pd.DataFrame(columns=['snils','last_name','first_name','patronymic','date_birth','person_id','uuid','result'])

snils = dataframe_etl['snils'].tolist()

last_name = dataframe_etl['last_name'].tolist()
first_name = dataframe_etl['first_name'].tolist()
patronymic = dataframe_etl['patronymic'].tolist()
date_birth = dataframe_etl['date_birth'].tolist()
gender = dataframe_etl['gender'].tolist()

snils = [item.replace(" ", "") for item in snils]
snils = [item.replace("-", "") for item in snils]

# rr = re.findall(r'(?s)(?<=ns2:Result>).*?(?=</ns2:Result)', response)[0]
# #  <typ:clientId>34d78de2-7643-4a79-9bd1-574b5c2a9d0a</typ:clientId>

for i in range(2):

    body="""<?xml version="1.0" encoding="UTF-8"?>
<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/">
   <soapenv:Header/>
   <soapenv:Body>
      <tns:ClientMessage xmlns:tns="urn://x-artefacts-smev-gov-ru/services/service-adapter/types">
         <tns:itSystem>77A609</tns:itSystem>
         <tns:RequestMessage>
            <tns:RequestMetadata>
               <tns:clientId>UUID</tns:clientId>
            </tns:RequestMetadata>
            <tns:RequestContent>
               <tns:content>
                  <tns:MessagePrimaryContent>
<tns:SnilsValidationRequest xmlns:tns="http://kvs.pfr.com/snils-validation/1.0.1" xmlns:smev="urn://x-artefacts-smev-gov-ru/supplementary/commons/1.0.1">
	<smev:FamilyName>ФАМИЛИЯ</smev:FamilyName>
	<smev:FirstName>ИМЯ</smev:FirstName>
	<smev:Patronymic>ОТЧЕСТВО</smev:Patronymic>
	<tns:Snils>СНИЛС</tns:Snils>
	<tns:Gender>ПОЛ</tns:Gender>
	<tns:BirthDate>ДАТАРОЖДЕНИЯ</tns:BirthDate>
</tns:SnilsValidationRequest>
                  </tns:MessagePrimaryContent>
               </tns:content>
            </tns:RequestContent>
         </tns:RequestMessage>
      </tns:ClientMessage>
   </soapenv:Body>
</soapenv:Envelope>"""
    body2="""<?xml version="1.0" encoding="utf-8"?>
<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:typ="urn://x-artefacts-smev-gov-ru/services/service-adapter/types">
   <soapenv:Header/>
   <soapenv:Body>
      <typ:FindMessageQuery>
        <typ:itSystem>77A609</typ:itSystem>
         <typ:specificQuery>
           <typ:messageClientIdCriteria>
               <typ:clientId>UUID</typ:clientId>
               <typ:clientIdCriteria>GET_RESPONSE_BY_REQUEST_CLIENTID</typ:clientIdCriteria>
               <!-- GET_REQUEST_BY_REQUEST_CLIENTID
                      GET_RESPONSE_BY_REQUEST_CLIENTID
                      GET_RESPONSE_BY_RESPONSE_CLIENTID-->
            </typ:messageClientIdCriteria>
         </typ:specificQuery>
      </typ:FindMessageQuery>
   </soapenv:Body>
</soapenv:Envelope>"""
    endpoint = "http://10.34.1.68:7575/ws/"
    

    snils2 = snils[i]
    last_name2 = last_name[i]
    first_name2 = first_name[i]
    patronymic2 = patronymic[i]
    date_birth2 = date_birth[i]
    gender2 = gender[i]

    uuidcode = uuid.uuid4()
    uuidcode = str(uuidcode)

    body = body.replace('ФАМИЛИЯ',last_name2)
    body = body.replace('ИМЯ',first_name2)
    body = body.replace('ОТЧЕСТВО',patronymic2)
    body = body.replace('СНИЛС',snils2)
    body = body.replace('ПОЛ',gender2)
    body = body.replace('ДАТАРОЖДЕНИЯ',date_birth2)
    body = body.replace('UUID',uuidcode)

    body2 = body2.replace('UUID',uuidcode)

    body = body.encode('utf-8')
    session = requests.session()
    session.headers = {"Content-Type": "text/xml; charset=utf-8"}
    response = session.post(url=endpoint, data=body, verify=False)

    print(response.content.decode("utf-8"))

    time.sleep(60)

    body2 = body2.encode('utf-8')
    session2 = requests.session()
    session2.headers = {"Content-Type": "text/xml; charset=utf-8"}
    response2 = session2.post(url=endpoint, data=body2, verify=False)

    # response = response.content.decode("utf-8")

    print(response2.content.decode("utf-8"))
    i = i + 1