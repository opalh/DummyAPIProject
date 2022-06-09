"""Store data from dummy API https://dummyapi.io/docs into MySQL database

The users must change the configurations' info in db_connection/connection_info.py
file before running this script:
-hostname
-username
-password
-dbname default value ("dummyApi")

This script fetch the following objects from https://dummyapi.io/ and does some cleaning
and validation:
- Users
- Posts
- Comments

After the data cleaning process, the script imports the data into the MySQL database
the user has set in the configs (with default value "dummyApi") in the following tables:
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
    the password for MySQL user account (default dummyApi)
dbname : str
    the name of the database the tables users, posts and comments will be created

# dummy data app-id
parameters : json
    the json contains the app-id needed to fetch the data

# MySQL tables schema
users_datatype : dict
    contains the columns and datatypes of the MySQL table users

posts_datatype : dict
    contains the columns and datatypes of the MySQL table posts

comments_datatype : dict
    contains the columns and datatypes of the MySQL table comments

Methods
-------
fetch_data_from_api(request_url, limit_value) - fetch the data from the given
    endpoint and returns a dataframe

clean_dataframe(df) - clean and validate the data from input dataframe and return
    a clean dataframe

drop_table_if_exists(engine_con, database_name, table_name) - drop the tables from MySQL
    if they already exists

upload_data_to_mysql_tables(engine_con, table_name, df, datatypes) - upload the data
    from the dataframe into MySQL's table

add_keys_and_constraints(engine_con) - add the private and foreign key constrains

main - the main function of the script
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
    """Fetch the data from the given endpoint and returns a dataframe

    Parameters
    ----------
    limit_value: int
        limit value should be in range [5-50]
    request_url: str
        request url to fetch the objects from the api

    Returns
    -------
    dataframe
        a dataframe with the data fetched from the api

    """
    page_number = 0
    length = 1
    df_data_prev = pd.DataFrame()
    df_data_all = pd.DataFrame()

    # fetch all the data from pages [0-999] withe limit [5-50]
    while length != 0:
        response = requests.get(request_url + "?page=" + str(page_number) + "&limit=" + str(limit_value),
                                headers=parameters)
        data_info = json.loads(response.text)['data']
        df_data_prev = pd.concat([df_data_prev, pd.DataFrame(data_info)])
        length = len(data_info)
        page_number += 1

    # fetch the users' and posts' full data
    if 'user' in request_url or 'post' in request_url:
        for i in df_data_prev['id']:
            response = requests.get(request_url + i, headers=parameters)
            data_info = json.loads(response.text)
            df_data_all = pd.concat([df_data_all, pd.DataFrame([data_info])])
    else:
        df_data_all = df_data_prev

    return df_data_all


def clean_dataframe(df):
    """Clean and validate the data from input dataframe and return a clean dataframe

     Parameters
     ----------
     df: dataframe
         a dataframe with the data fetched from the api

     Returns
     -------
     dataframe
         a dataframe containing data after the cleaning and validation

     """

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

    # format the datetime column publishDate from post data
    if 'publishDate' in df.columns:
        df.publishDate = pd.to_datetime(df.publishDate, format='%Y-%m-%d %H:%M:%S')

    # unnest location info and store it into different columns in the dataframe
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
    """Drop the tables from MySQL if they already exists

    Parameters
    ----------
    engine_con: variable
        connection object
    database_name: str
        database name
    table_name: str
        the name of the table
    """
    # drop tables if exists
    engine_con.execute("DROP TABLE IF EXISTS " + database_name + "." + table_name + ";")


def upload_data_to_mysql_tables(engine_con, table_name, df, datatypes):
    """Upload the data from the dataframe into MySQL's table

    Parameters
    ----------
    engine_con: variable
        connection object
    table_name: str
        the name of the table
    df: dataframe
        a dataframe with the data to insert into mysql
    datatypes: json
        a json with the schema of MySQL's table
    """

    # insert users data into MySQL
    df.to_sql(con=engine_con, name=table_name, if_exists='replace', index=False, dtype=datatypes)


def add_keys_and_constraints(engine_con):
    """Add the private and foreign key constrains to MySQL tables created

    Parameters
    ----------
    engine_con: variable
        connection object
    """
    # add id as a primary key
    engine_con.execute("ALTER TABLE `users`  ADD PRIMARY KEY (`id`);")
    engine_con.execute("ALTER TABLE `posts` ADD PRIMARY KEY (`id`);")
    # add foreign keys of the owner's for the posts and the comments
    engine_con.execute(
        "ALTER TABLE `posts` ADD CONSTRAINT `fk_posts_owner` FOREIGN KEY (ownerId) REFERENCES `users` (id);")
    engine_con.execute("ALTER TABLE `comments` ADD PRIMARY KEY (`id`);")
    engine_con.execute(
        "ALTER TABLE `comments` ADD CONSTRAINT `fk_comments_owner` FOREIGN KEY (ownerId)  REFERENCES `users` (id);")
    # add foreign key connecting comments and posts
    # engine_con.execute(
    #     "ALTER TABLE `comments` ADD CONSTRAINT `fk_comments_postid` FOREIGN KEY (post)  REFERENCES `posts` (id);")


def main():
    # fetch user's data
    users_df = fetch_data_from_api("https://dummyapi.io/data/v1/user/", 10)

    # clean user's data
    clean_users = clean_dataframe(users_df)

    # fetch posts' data
    posts_df = fetch_data_from_api("https://dummyapi.io/data/v1/post/", 20)

    # clean posts' data
    clean_posts = clean_dataframe(posts_df)

    # fetch comments' data
    comments_df = fetch_data_from_api("https://dummyapi.io/data/v1/comment/", 20)

    # clean comments' data
    clean_comments = clean_dataframe(comments_df)

    # create database to MySQL
    start_engine.execute("CREATE DATABASE IF NOT EXISTS " + dbname + ";")

    # drop tables from database if already exist
    drop_table_if_exists(engine, dbname, 'posts')

    drop_table_if_exists(engine, dbname, 'comments')

    drop_table_if_exists(engine, dbname, 'users')

    # insert users' data to MySQL
    upload_data_to_mysql_tables(engine, 'users', clean_users, users_datatype)

    # insert posts' data to MySQL
    upload_data_to_mysql_tables(engine, 'posts', clean_posts, posts_datatype)

    # insert comments' data to MySQL
    upload_data_to_mysql_tables(engine, 'comments', clean_comments, comments_datatype)

    # format tables add keys and constraints to MySQL's tables
    add_keys_and_constraints(engine)


if __name__ == '__main__':
    main()
