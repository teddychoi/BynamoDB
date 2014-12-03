import os
import shutil
import subprocess

from boto.dynamodb2.layer1 import DynamoDBConnection

from bynamodb import patch_dynamodb_connection


process = None

def pytest_configure():
    global process
    shutil.rmtree('dynamodb/testdb', True)
    os.mkdir('dynamodb/testdb')

    host = 'localhost'
    port = 8001
    dev_null = open(os.devnull, 'wb')
    process = subprocess.Popen([
        '/usr/bin/java', '-Djava.net.preferIPv4Stack=true',
        '-Djava.library.path=./dynamodb/DynamoDBLocal_lib', '-jar',
        'dynamodb/DynamoDBLocal.jar', '-port', str(port),
        '-dbPath', './dynamodb/testdb'
    ], stdout=dev_null, stderr=dev_null)

    patch_dynamodb_connection(host=host, port=port, is_secure=False)
    conn = DynamoDBConnection()
    assert conn.host == host
    assert conn.port == port


def pytest_runtest_teardown():
    conn = DynamoDBConnection()
    table_names = conn.list_tables()['TableNames']
    for table_name in table_names:
        conn.delete_table(table_name)


def pytest_unconfigure():
    process.terminate()
