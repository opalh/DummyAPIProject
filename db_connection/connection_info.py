from sqlalchemy import create_engine

# mysql parameters
hostname = "localhost"
uname = "irene"
pwd = "test123"
dbname = "dummyApi"

# dummy data app-id
parameters = {
    "app-id": "629c7216cdf998caab5c026f"
}

# connection objects
engine = create_engine(
        "mysql+pymysql://{user}:{pw}@{host}/{db}".format(host=hostname, db=dbname, user=uname, pw=pwd))

start_engine = create_engine(
        "mysql+pymysql://{user}:{pw}@{host}/".format(host=hostname, user=uname, pw=pwd))
