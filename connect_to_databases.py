import psycopg2
import psycopg2.extras
import pandas.io.sql as psql
from clickhouse_driver import Client
import hvac
from dotenv import load_dotenv
import warnings
import pandas as pd
import os
import numpy as np


def get_secret_from_vault(mount_point, path):
    try:
        load_dotenv(".env")
        client = hvac.Client(url=os.environ['ip'])
        client.auth_userpass(username=os.environ['user'], password=os.environ['password'])
        read_response = client.secrets.kv.read_secret_version(path=path, mount_point=mount_point)
        secret = read_response['data']['data']
        return secret
    except Exception as e:
        print(f"ERROR in get_secret() {e}")
        raise e

def gp_connect_to_db(secret):
    try:
        db = psycopg2.connect(dbname=secret['dbname'],
                                    user=secret['username'],
                                    password=secret['password'],
                                    host=secret['host'],
                                    port=secret['port'])
        return db
    except psycopg2.DatabaseError as e:
        print("could not connect to Greenplum server",e)
        
"""How to connect"""
secret_db = get_secret_from_vault('database', 'greenplum/username')
connection_db = gp_connect_to_db(secret_db)
secret_clickhouse = get_secret_from_vault('database', 'clickhouse/username')
client = Client("ip_address", port="port", user=secret_clickhouse['user'], password=secret_clickhouse['password'], settings={'use_numpy': True})