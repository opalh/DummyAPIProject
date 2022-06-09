"""Store data from dummy API https://dummyapi.io/docs into MySQL database

The users must change the configurations' info in db_connection/connection_info.py
file before running this script:
-hostname
-username
-password
-dbname default value ("learnWorlds")

This script fetch the following objects from https://dummyapi.io/ and does some cleaning
and validation:
- Users
- Posts
- Comments

After the data cleaning process, the script imports the data into the MySQL database
the user has set in the configs (with default value "learnWorlds") in the following tables:
- users
- posts
- comments

This script requires that the follwoing packages will be installed within the Python
environment you are running this script in.
- json
- pandas
- requests
- sqlalchemy
from db_connection.connection_info import engine, start_engine, dbname

This file contains the following attributes and methods:

Attributes from db_connection/connection_info.py file
----------

# mysql parameters
hostname : str
    the hostname for MySQL database
uname : str
    the username for MySQL user account
pwd : str
    the password for MySQL user account
dbname : str
    the name of the database the tables users, posts and comments will be created

# dummy data app-id
parameters : json
    the json contains the app-id needed to fetch the data

engine = create_engine(
        "mysql+pymysql://{user}:{pw}@{host}/{db}".format(host=hostname, db=dbname, user=uname, pw=pwd))

start_engine = create_engine(
        "mysql+pymysql://{user}:{pw}@{host}/".format(host=hostname, user=uname, pw=pwd))

says_str : str
    a formatted string to print out what the animal says
name : str
    the name of the animal
sound : str
    the sound that the animal makes
num_legs : int
    the number of legs the animal has (default 4)

Methods
-------
says(sound=None)
    Prints the animals name and what sound it makes
"""

import json
import pandas as pd
import requests
from sqlalchemy.types import NVARCHAR, INT, DATETIME, TEXT
from db_connection.connection_info import engine, start_engine, dbname, parameters


# mysql tables schema
users_datatype = dict(
    {"id": NVARCHAR(255), "title": NVARCHAR(255), "firstName": NVARCHAR(255), "lastName": NVARCHAR(255),
     "email": NVARCHAR(500),
     "picture": NVARCHAR(500), "dateOfBirth": DATETIME, "registerDate": DATETIME, "updatedDate": DATETIME, })

posts_datatype = dict(
    {"id": NVARCHAR(255), "likes": INT, "tags": TEXT, "text": TEXT, "publishDate": DATETIME, "ownerId": NVARCHAR(255),
     "link": TEXT
     })
comments_datatype = dict(
    {"id": NVARCHAR(255), "message": TEXT, "post": NVARCHAR(255), "publishDate": DATETIME, "ownerId": NVARCHAR(255)
     })


def fetch_data_from_api(request_url, limit_value):
    page_number = 0
    length = 1
    df_data_prev = pd.DataFrame()
    df_data_all = pd.DataFrame()

    while length != 0:
        response = requests.get(request_url + "?page=" + str(page_number) + "&limit=" + str(limit_value),
                                headers=parameters)
        data_info = json.loads(response.text)['data']
        df_data_prev = pd.concat([df_data_prev, pd.DataFrame(data_info)])
        length = len(data_info)
        page_number += 1

    if 'user' in request_url or 'post' in request_url:
        for i in df_data_prev['id']:
            response = requests.get(request_url + i, headers=parameters)
            data_info = json.loads(response.text)
            df_data_all = pd.concat([df_data_all, pd.DataFrame([data_info])])
    else:
        df_data_all = df_data_prev

    return df_data_all


def clean_dataframe(df):
    # drop duplicate rows in the dataframe
    df = df.drop_duplicates(subset=['id'])

    # drop columns that has null in id column
    df = df.dropna(subset=['id'])

    # drop rows without an owner, only for the dataframes that includes this column
    # extract owner_id value from the json format column owner
    if 'owner' in df.columns:
        df = df.dropna(subset=['owner'])
        df['ownerId'] = df['owner'].str['id']
        df = df.drop(['owner'], axis='columns')

    # drop rows with negative value for likes, only for the dataframes that includes this column
    if 'likes' in df.columns:
        df = df.drop(df.index[df['likes'] < 0])

    # fix format of tags columns from list to json to fix an issue with the insert into mysql
    if 'tags' in df.columns:
        df['tags'] = df['tags'].apply(json.dumps)

    if 'publishDate' in df.columns:
        df.publishDate = pd.to_datetime(df.publishDate, format='%Y-%m-%d %H:%M:%S')

    if 'location' in df.columns:
        df['loc_street'] = df['location'].str['street']
        df['loc_city'] = df['location'].str['city']
        df['loc_state'] = df['location'].str['state']
        df['loc_country'] = df['location'].str['country']
        df['loc_timezone'] = df['location'].str['timezone']
        df = df.drop(['location'], axis='columns')
        df.dateOfBirth = pd.to_datetime(df.dateOfBirth, format='%Y-%m-%d %H:%M:%S')
        df.registerDate = pd.to_datetime(df.registerDate, format='%Y-%m-%d %H:%M:%S')
        df.updatedDate = pd.to_datetime(df.updatedDate, format='%Y-%m-%d %H:%M:%S')

    return df


def drop_table_if_exists(engine_con, database_name, table_name):
    # drop fk tables
    engine_con.execute("DROP TABLE IF EXISTS " + database_name + "." + table_name + ";")


def upload_data_to_mysql_tables(engine_con, table_name, df, datatypes):
    # insert users data
    df.to_sql(con=engine_con, name=table_name, if_exists='replace', index=False, dtype=datatypes)


def add_keys_and_constraints(engine_con):
    # add is as a primary key
    engine_con.execute("ALTER TABLE `users`  ADD PRIMARY KEY (`id`);")
    engine_con.execute("ALTER TABLE `posts` ADD PRIMARY KEY (`id`);")
    engine_con.execute(
        "ALTER TABLE `posts` ADD CONSTRAINT `fk_posts_owner` FOREIGN KEY (ownerId) REFERENCES `users` (id);")
    engine_con.execute("ALTER TABLE `comments` ADD PRIMARY KEY (`id`);")
    engine_con.execute(
        "ALTER TABLE `comments` ADD CONSTRAINT `fk_comments_owner` FOREIGN KEY (ownerId)  REFERENCES `users` (id);")
    # engine_con.execute(
    #     "ALTER TABLE `comments` ADD CONSTRAINT `fk_comments_postid` FOREIGN KEY (post)  REFERENCES `posts` (id);")


def main():
    # fetch the data
    users_df = fetch_data_from_api("https://dummyapi.io/data/v1/user/", 10)

    # clean the data
    clean_users = clean_dataframe(users_df)

    # fetch the data
    posts_df = fetch_data_from_api("https://dummyapi.io/data/v1/post/", 20)

    # clean the data
    clean_posts = clean_dataframe(posts_df)

    # fetch the data
    comments_df = fetch_data_from_api("https://dummyapi.io/data/v1/comment/", 20)

    # clean the data
    clean_comments = clean_dataframe(comments_df)

    # create database
    start_engine.execute("CREATE DATABASE IF NOT EXISTS " + dbname + ";")

    # drop tables if already exist
    drop_table_if_exists(engine, dbname, 'posts')

    drop_table_if_exists(engine, dbname, 'comments')

    drop_table_if_exists(engine, dbname, 'users')

    # insert users data
    upload_data_to_mysql_tables(engine, 'users', clean_users, users_datatype)

    # insert posts data
    upload_data_to_mysql_tables(engine, 'posts', clean_posts, posts_datatype)

    # insert comments data
    upload_data_to_mysql_tables(engine, 'comments', clean_comments, comments_datatype)

    # format tables add keys and constraints
    add_keys_and_constraints(engine)


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    main()
