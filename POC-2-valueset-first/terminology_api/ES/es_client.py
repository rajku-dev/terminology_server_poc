import os
from elasticsearch import Elasticsearch
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

es = Elasticsearch(
    os.getenv("ES_HOST"),
    basic_auth=(
        os.getenv("ES_USERNAME"),
        os.getenv("ES_PASSWORD")
    ),
    verify_certs=False
)
