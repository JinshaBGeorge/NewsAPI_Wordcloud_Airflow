######################################################
## Data Pipeline to extract, transform and load data.
## Stage 1: Extract Data --> Set API connection, extract sources and no. of sources, 
##                           extract articles depending on initial or incremental load and store to dataframe.
## Stage 2: Data Cleaning --> Split columns, Truncate spaces, Fill Nan, Change column names, rearrange columns, change timestamp format
## Stage 3: Transform Data --> Split dataframe into Dimensions and Facts to form a star schema
## Stage 4: Load Data --> Load data into postgreSQL
## Pipeline: Calls all the functions in the intended order
## Runs Pipeline through airflow
#######################################################

#Importing libraries
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from datetime import *
import pandas as pd
import numpy as np
from newsapi import NewsApiClient
import psycopg2
from sqlalchemy import create_engine
from uuid import uuid4

#Connecting to API
API_KEY='0c481110c0134583b279b55d3baa7588'
newsapi = NewsApiClient(api_key=API_KEY)

#Function to create tables in pagila DB
def initial_table_creation():
    
    conn_details = psycopg2.connect(
    host="data-sandbox.c1tykfvfhpit.eu-west-2.rds.amazonaws.com",
    database="pagila",
    user="de8_jige15",
    password="CBdwk29)",
    port= '5432'
    )

    cursor = conn_details.cursor()
    dt_table_creation = '''
    CREATE TABLE student.jbg_datetime (
        datetime_id varchar(12) PRIMARY KEY,
        published_at timestamp
    )
    '''
    ns_table_creation = '''
    CREATE TABLE student.jbg_newssource (
        news_source_id varchar(12) PRIMARY KEY, 
        news_domain_id varchar(50), 
        news_domain_name varchar(50)
    )
    '''
    au_table_creation = '''
    CREATE TABLE student.jbg_author (
        author_id varchar(12) PRIMARY KEY,
        author varchar(200)
    )
    '''
    ct_table_creation = '''
    CREATE TABLE student.jbg_content (
        content_id varchar(12) PRIMARY KEY, 
        title varchar(1000),
        news_url varchar(1000)
    )
    '''
    at_table_creation = '''
    CREATE TABLE student.jbg_articles (
        articles_id varchar(12) PRIMARY KEY, 
        datetime_id varchar(12),
        news_source_id varchar(12),
        author_id varchar(12),
        content_id varchar(12),
        CONSTRAINT fk_datetime
        FOREIGN KEY(datetime_id) 
            REFERENCES jbg_datetime(datetime_id),
        CONSTRAINT fk_newssoruce
        FOREIGN KEY(news_source_id) 
            REFERENCES jbg_newssource(news_source_id),
        CONSTRAINT fk_author
        FOREIGN KEY(author_id) 
            REFERENCES jbg_author(author_id),
        CONSTRAINT fk_content
        FOREIGN KEY(content_id) 
            REFERENCES jbg_content(content_id)
    )
    '''

    cursor.execute(dt_table_creation)
    cursor.execute(ns_table_creation)
    cursor.execute(au_table_creation)
    cursor.execute(ct_table_creation)
    cursor.execute(at_table_creation)
    conn_details.commit()
    cursor.close()
    conn_details.close()

#Function to fetch the UK news sources
def fetch_sources():
    sources = newsapi.get_sources(country = 'gb')
    sources_list = []
    for source in sources['sources']:
        sources_list.append(source['id'])
    return sources_list, len(sources_list)

#Function to receive the JSON response from API
def get_response_json(from_date, source, page_size, page):
    response_json = newsapi.get_everything( language='en',
                                            from_param=str(from_date),
                                            to= str(date.today()),
                                            sources = source,
                                            page_size=page_size,
                                            page = page,
                                            sort_by='publishedAt')
    return response_json


#Function to extract articles - Checks initial/incremental load
def extract_articles(sources_list,no_of_sources):
    
    conn_details = psycopg2.connect(
    host="data-sandbox.c1tykfvfhpit.eu-west-2.rds.amazonaws.com",
    database="pagila",
    user="de8_jige15",
    password="CBdwk29)",
    port= '5432'
    )
    cursor = conn_details.cursor()
    cursor.execute(''' SELECT MAX(published_at) FROM student.jbg_datetime ''')
    results = cursor.fetchall()
    results = [res for tup in results for res in tup]

    if results[0] is not None:
        from_date = results[0].date()
    else:
       n_days = 30
       from_date = date.today() - timedelta(days=n_days)

    page = 1
    page_size = 100
    results = []

    for i in range(no_of_sources):
        results.append(get_response_json(from_date, sources_list[i], page_size, page))

    articles = pd.DataFrame()
    for rows in range(len(results)):
        articles = articles.append(results[rows]['articles'],ignore_index=True)
    
    return articles

#Function used to clean the data
def data_cleaning(articles):
    articles = pd.concat([articles.drop(['source'], axis=1), articles['source'].apply(pd.Series)], axis=1)
    #Index(['title', 'author', 'publishedAt', 'url', 'id', 'name'], dtype='object')
    print(articles.columns)
    articles['id'].str.strip()
    articles['name'].str.strip()
    articles['publishedAt'].str.strip()
    articles['title'].str.strip()
    articles['author'].str.strip()
    articles['url'].str.strip()
    articles.fillna(np.nan).replace([np.nan], [None])
    articles = articles[['id','name','publishedAt','title','author','url']]
    articles.columns = ['news_domain_id','news_domain_name','published_at','title','author','news_url']
    articles['published_at'] = articles['published_at'].astype('datetime64[ns]')

    return articles

#Function to check if a datetime exists in thr DB before data loading
def check_exists_datetime(value):
  conn_details = psycopg2.connect(
   host="data-sandbox.c1tykfvfhpit.eu-west-2.rds.amazonaws.com",
   database="pagila",
   user="de8_jige15",
   password="CBdwk29)",
   port= '5432'
  )
  cursor = conn_details.cursor()
  cursor.execute(''' SELECT datetime_id FROM student.jbg_datetime WHERE published_at= %s ''', (value,))
  results = cursor.fetchall()
  results = [res for tup in results for res in tup]
  row_count = cursor.rowcount
  
  conn_details.commit()
  cursor.close()
  conn_details.close()

  if row_count == 0:
    return 'DT'+ uuid4().hex[:10]
  else:
    return results[0]+'_OLD'


#Function to check if a news source exists in thr DB before data loading
def check_exists_source(value):
  conn_details = psycopg2.connect(
   host="data-sandbox.c1tykfvfhpit.eu-west-2.rds.amazonaws.com",
   database="pagila",
   user="de8_jige15",
   password="CBdwk29)",
   port= '5432'
  )
  cursor = conn_details.cursor()
  cursor.execute(''' SELECT news_source_id FROM student.jbg_newssource WHERE news_domain_id = %s ''', (value,))
  results = cursor.fetchall()
  results = [res for tup in results for res in tup]
  row_count = cursor.rowcount
  
  conn_details.commit()
  cursor.close()
  conn_details.close()
  
  if row_count == 0:
    return 'NS'+ uuid4().hex[:10]
  else:
    return results[0]+'_OLD'


#Function to check if an author exists in thr DB before data loading
def check_exists_author(value):
  conn_details = psycopg2.connect(
    host="data-sandbox.c1tykfvfhpit.eu-west-2.rds.amazonaws.com",
    database="pagila",
    user="de8_jige15",
    password="CBdwk29)",
    port= '5432'
  )
  cursor = conn_details.cursor()
  cursor.execute(''' SELECT author_id FROM student.jbg_author WHERE author = %s ''', (value,))
  results = cursor.fetchall()
  results = [res for tup in results for res in tup]
  row_count = cursor.rowcount
  
  conn_details.commit()
  cursor.close()
  conn_details.close()

  if row_count == 0:
    return 'AU'+ uuid4().hex[:10]
  else:
    return results[0]+'_OLD'

#Function used to design a star schema - datetime, newssource, author, content, articles
def transform_articles(articles_df):
    datetime_df = articles_df[['published_at']].drop_duplicates()
    datetime_df['datetime_id'] = datetime_df['published_at'].apply(check_exists_datetime)
    datetime_df = datetime_df[['datetime_id', 'published_at']]

    articles_df = pd.merge(articles_df,datetime_df,how = 'left', left_on = 'published_at', right_on = 'published_at')
    articles_df = articles_df.drop(columns = ['published_at'],axis = 1)

    news_source_df = articles_df[['news_domain_id', 'news_domain_name']].drop_duplicates()
    news_source_df['news_source_id'] = news_source_df['news_domain_id'].apply(check_exists_source)
    news_source_df = news_source_df[['news_source_id', 'news_domain_id', 'news_domain_name']]

    articles_df = pd.merge(articles_df,news_source_df,how = 'left', left_on = ['news_domain_id', 'news_domain_name'], right_on = ['news_domain_id', 'news_domain_name'])
    articles_df = articles_df.drop(columns = ['news_domain_id', 'news_domain_name'],axis = 1)

    author_df = articles_df[['author']].drop_duplicates()
    author_df['author_id'] = author_df['author'].apply(check_exists_author)
    author_df = author_df[['author_id', 'author']]

    articles_df = pd.merge(articles_df,author_df,how = 'left', left_on = ['author'], right_on = ['author'])
    articles_df = articles_df.drop(columns = ['author'],axis = 1)

    content_df = articles_df[['title','news_url']].drop_duplicates()
    content_df['content_id'] = content_df.index.map(lambda _: 'CT'+ uuid4().hex[:10])
    content_df = content_df[['content_id','title','news_url']]

    articles_df = pd.merge(articles_df,content_df,how = 'left', left_on = ['title','news_url'], right_on = ['title','news_url'])
    articles_df = articles_df.drop(columns = ['title','news_url'],axis = 1)
    articles_df['articles_id'] = articles_df.index.map(lambda _: 'AR'+ uuid4().hex[:10])
    articles_df = articles_df[['articles_id','datetime_id','news_source_id','author_id','content_id']]

    datetime_df = datetime_df[datetime_df["datetime_id"].str.contains("_OLD") == False]
    news_source_df = news_source_df[news_source_df["news_source_id"].str.contains("_OLD") == False]
    author_df = author_df[author_df["author_id"].str.contains("_OLD") == False]
    articles_df = articles_df.replace('_OLD','', regex=True)

    return articles_df, datetime_df, news_source_df, author_df, content_df

#Funtion which loads the data into DB
def load_articles(datetime_df,news_source_df,author_df,content_df,articles_df):
    conn_string = 'postgresql://de8_jige15:CBdwk29)@data-sandbox.c1tykfvfhpit.eu-west-2.rds.amazonaws.com:5432/pagila'
    db = create_engine(conn_string)
    conn = db.connect()
    
    datetime_df.to_sql('jbg_datetime', con=conn, schema = 'student',if_exists='append',index=False)
    news_source_df.to_sql('jbg_newssource', con=conn,schema = 'student', if_exists='append',index=False)
    author_df.to_sql('jbg_author', con=conn,schema = 'student', if_exists='append',index=False)
    content_df.to_sql('jbg_content', con=conn,schema = 'student', if_exists='append',index=False)
    articles_df.to_sql('jbg_articles', con=conn,schema = 'student', if_exists='append',index=False)

#checks if it is incremental or initial load
def check_incremental_load():
    try:
      initial_table_creation()
    except:
       print("Incremental data load....")

#Data pipeline function for ETL
def etl_pipeline():
    sources, no_of_sources = fetch_sources()
    articles_df = extract_articles(sources, no_of_sources)
    articles_df = data_cleaning(articles_df)
    articles_df, datetime_df, news_source_df, author_df, content_df = transform_articles(articles_df)
    load_articles(datetime_df,news_source_df,author_df,content_df,articles_df)
   
def pipeline_complete():
   print('Data Loaded Successfully...')



##########################################################
##          AIRFLOW SETUP AND DAGS DEFINED
##########################################################
    
args = {
 
    'owner': 'jinsha',
    'start_date': days_ago(1)
}
 
 
args = {
 
    'owner': 'jinsha',
    'start_date': days_ago(1),
    'depends_on_past': False,
    'catchup_by_default': False,
}
 
 
dag = DAG(dag_id = 'data_pipeline_API_SQL', default_args=args, schedule_interval='@daily', catchup =False)

checkincremental = PythonOperator(
    task_id="checkincremental",
    provide_context=True,
    python_callable=check_incremental_load,
    dag=dag,
)
etlpipeline = PythonOperator(
    task_id="etlpipeline",
    provide_context=True,
    python_callable=etl_pipeline,
    dag=dag,
)

pipelinestatus = PythonOperator(
    task_id="pipelinestatus",
    provide_context=True,
    python_callable=pipeline_complete,
    dag=dag,
)

checkincremental >> etlpipeline >>  pipelinestatus