
import os

import requests

import toml

from importlib_resources import files



CONFIG_PATH = "config"





def get_config(env):

    # Read COS config from toml config file



    config_text = files(CONFIG_PATH).joinpath("connection_config.toml").read_text()

    config = toml.loads(config_text)

    cos_config = config["LAMBDA_CONFIG"][f"{env}"]

    return cos_config, config





def get_client_id(config):

    # Use Vault client

    # Service is started through configuration of lambda following instructions from COS documentation

    # print(config)

    response = requests.get(

        f"http://127.0.0.1:8200/v1/{config['LOCK_BOX']}/{config['CLIENT_ID_PATH']}"

    )

    client_id = response.json()["data"][config["CLIENT_KEY"]]

    return client_id





def get_client_secret(config):

    response = requests.get(

        f"http://127.0.0.1:8200/v1/{config['LOCK_BOX']}/{config['CLIENT_SECRET_PATH']}"

    )



    client_secret = response.json()["data"][config["CLIENT_SECRET_KEY"]]

    return client_secret





def get_token(env):

    cos_config, config = get_config(env)



    # Create data payload to request token

    data = dict(

        client_id=get_client_id(cos_config),

        client_secret=get_client_secret(cos_config),

        grant_type="client_credentials",

    )



    # Request token

    host = config["API_ENDPOINTS"][f"{env}_ENDPOINT"]

    url = f"{host}/oauth2/token"

    try:

        response = requests.post(url, data=data)

    except requests.exceptions.SSLError:

        print("Getting Certificate and appending to cert file!")

        # Copy certifi file into temporary directory

        os.system("cat $(python -m certifi) > /tmp/cacert.pem")



        # Get certificate from artifactory

        artifactory_url = "artifactory.cloud.capitalone.com"

        bundle_string = f"https://{artifactory_url}/artifactory/generic-internalfacing/cof-ca-certs/cof_bundle.pem"

        response = requests.get(bundle_string)



        # Append response from artifactory to cert file

        with open("/tmp/cacert.pem", "a") as f:

            f.write(str(response.text))



        # Set requests bundle location

        os.environ["REQUESTS_CA_BUNDLE"] = "/tmp/cacert.pem"



        response = requests.post(url, data=data)



    if response.status_code > 299:

        raise ValueError(

            f"Unable to get token: {response.status_code} - {response.json()['error']}"

        )



    return response.json()["access_token"]

