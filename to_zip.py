from elasticsearch import Elasticsearch, helpers
import json
import tarfile
import io

es = Elasticsearch("http://localhost:9200")

index_name = "control_flow_graph_index"
output_tar_gz = "cfgs_with_id.tar.gz"
output_json_inside_tar = "cfgs_with_id.json"

def fetch_all_docs(es_client, index):
    query = {"query": {"match_all": {}}}
    docs = []
    
    for doc in helpers.scan(es_client, index=index, query=query):
        d = doc["_source"]
        d["_id"] = doc['_id']
        docs.append(d)
    
    return docs

all_docs = fetch_all_docs(es, index_name)


with tarfile.open(output_tar_gz, mode="w:gz") as tar:
    json_bytes = json.dumps(all_docs, ensure_ascii=False, indent=4).encode('utf-8')
    json_file = io.BytesIO(json_bytes)
    
    info = tarfile.TarInfo(name=output_json_inside_tar)
    info.size = len(json_bytes)
    
    tar.addfile(tarinfo=info, fileobj=json_file)

print(f"Downloaded {len(all_docs)} indexes and saved in {output_tar_gz}")
