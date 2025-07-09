import os
from dotenv import load_dotenv
from elasticsearch import Elasticsearch

load_dotenv()

es = Elasticsearch(
    os.getenv("ES_HOST"),
    basic_auth=(
        os.getenv("ES_USERNAME"),
        os.getenv("ES_PASSWORD")
    ),
    verify_certs=False
)
