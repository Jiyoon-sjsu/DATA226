import pandas as pd
from vespa.application import Vespa
from vespa.io import VespaResponse, VespaQueryResponse

def display_hits_as_df(response: VespaQueryResponse, fields) -> pd.DataFrame:
    records = []
    for hit in response.hits:
        record = {}
        for field in fields:
            record[field] = hit["fields"][field]
        records.append(record)
    return pd.DataFrame(records)

def keyword_search(app, search_query):
    query = {
        "yql": "select * from sources * where userQuery() limit 5",
        "query": search_query,
        "ranking": "bm25",
    }
    response = app.query(query)
    return display_hits_as_df(response, ["doc_id", "title"])

def semantic_search(app, query):
    query = {
        "yql": "select * from sources * where ({targetHits:100}nearestNeighbor(embedding,e)) limit 5",
        "query": query,
        "ranking": "semantic",
        "input.query(e)": "embed(@query)"
    }
    response = app.query(query)
    return display_hits_as_df(response, ["doc_id", "title"])

def get_embedding(doc_id):
    query = {
        "yql" : f"select doc_id, title, text, embedding from content.doc where doc_id contains '{doc_id}'",
        "hits": 1
    }
    result = app.query(query)
    
    if result.hits:
        return result.hits[0]
    return None

def query_movies_by_embedding(embedding_vector):
    query = {
        'hits': 5,
        'yql': 'select * from content.doc where ({targetHits:5}nearestNeighbor(embedding, user_embedding))',
        'ranking.features.query(user_embedding)': str(embedding_vector),
        'ranking.profile': 'recommendation'
    }
    return app.query(query)

app = Vespa(url="http://localhost", port=8082)

query = "The Chronicles of Narnia"

df = keyword_search(app, query)
print(df.head())

df = semantic_search(app, query)
print(df.head())

emb = get_embedding("2454")
results = query_movies_by_embedding(emb["fields"]["embedding"])
df = display_hits_as_df(results, ["doc_id", "title", "text"])
print(df.head())