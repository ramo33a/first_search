import toml

import requests

from importlib_resources import files

from sqlalchemy import create_engine

import psycopg2

CONFIG_PATH = "config"





def get_db_user(cos_config, db_config):

    # retrieve username for lockbox

    response = requests.get(

        f"http://127.0.0.1:8200/v1/{cos_config['LOCK_BOX']}/{db_config['USER_PATH']}"

    )

    db_user = response.json()["data"][db_config["USER_KEY"]]

    return db_user





def get_dbp(cos_config, db_config):

    response1 = requests.get(

        f"http://127.0.0.1:8200/v1/{cos_config['LOCK_BOX']}/{db_config['SECRET_PATH']}"

    )

    dbp = response1.json()["data"][db_config["SECRET_KEY"]]

    return dbp





def get_db_connection(env, env_for_db):

    config_text = files(CONFIG_PATH).joinpath("connection_config.toml").read_text()

    config = toml.loads(config_text)

    cos_config = config["LAMBDA_CONFIG"][f"{env}"]

    db_config = config["DB_CONFIG"][f"{env_for_db}"]

    db_name = db_config["DBNAME"]

    db_port = db_config["PORT"]

    db_host = db_config["HOST"]

    db_user = get_db_user(cos_config, db_config)

    dbp = get_dbp(cos_config, db_config)



    conn = None

    try:

        # conn = psycopg2.connect(host=db_host, user=db_user, password=dbp, port=db_port, database=db_name)

        engine_string = "postgresql://{username}:{password}@{host}:{port}/{database}".format(

            username=db_user,

            password=dbp,

            host=db_host,

            port=db_port,

            database=db_name,

        )



        engine = create_engine(engine_string)

        return engine



    except Exception as e:

        print("Database connection failed due to {}".format(e))



    return conn





def get_alternate_connection(env, env_for_db):

    config_text = files(CONFIG_PATH).joinpath("connection_config.toml").read_text()

    config = toml.loads(config_text)

    cos_config = config["LAMBDA_CONFIG"][f"{env}"]

    db_config = config["DB_CONFIG"][f"{env_for_db}"]

    db_name = db_config["DBNAME"]

    db_port = db_config["PORT"]

    db_host = db_config["HOST"]

    db_user = get_db_user(cos_config, db_config)

    dbp = get_dbp(cos_config, db_config)



    conn = psycopg2.connect(

        host=db_host,

        database=db_name,

        user=db_user,

        password=dbp,

        port=db_port)



    return conn