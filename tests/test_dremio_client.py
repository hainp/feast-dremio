import os

from feast_dremio.client import Client


def test_dremio_client_e2e():
    user = os.getenv('DREMIO_USER')
    passwd = os.getenv('DREMIO_PASSWORD')
    # Connect to Dremio Arrow Flight server endpoint.
    client = Client("localhost", 32010, user, passwd, False, None, True)
    client.connect()
    reader = client.execute_query(query='SELECT * FROM demo.feast.driver_stats')
    df = reader.read_pandas(timestamp_as_object=True)
    x, y = df.shape

    assert x > 0
    assert y > 0
