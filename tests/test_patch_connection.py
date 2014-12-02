from boto.dynamodb2.layer1 import DynamoDBConnection

from bynamodb import patch_dynamodb_connection


def test_patch_connection():
    patch_dynamodb_connection(
        host='localhost',
        port=8000
    )

    conn = DynamoDBConnection()
    assert conn.host == 'localhost'
    assert conn.port == 8000
