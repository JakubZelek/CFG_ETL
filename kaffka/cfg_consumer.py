import json
import hashlib
import networkx as nx
from kafka import KafkaConsumer
from networkx.algorithms import isomorphism
from elasticsearch import Elasticsearch, NotFoundError
import logging

ES = Elasticsearch("http://localhost:9200")
ISOMORPHISM_INDEX = "control_flow_graph_isomorphism_index"
CFG_INDEX = "control_flow_graph_index"
SCROLL_SIZE = 1000

KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "cfg_graphs"

CONSUMER = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="cfg_processor",
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
)

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)


def get_graph_properties(graph_dict):
    G = nx.DiGraph(graph_dict)

    num_edges = G.number_of_edges()

    out_degrees = sorted(dict(G.out_degree()).values())
    out_degrees = [str(i) for i in out_degrees]
    in_degrees = sorted(dict(G.in_degree()).values())
    in_degrees = [str(i) for i in in_degrees]

    return num_edges, out_degrees, in_degrees


def get_isomorphism_query(num_edges: int, out_degrees: str, in_degrees: str):
    query = {
        "query": {
            "bool": {
                "must": [
                    {"term": {"num_edges": num_edges}},
                    {"term": {"out_degrees.keyword": out_degrees}},
                    {"term": {"in_degrees.keyword": in_degrees}},
                ]
            }
        }
    }
    return query


def get_single_cfg_will_be_loaded_to_elasticsearch(cfg_file):
    global_template = {
        "filename": cfg_file["filename"],
        "commit_hash": cfg_file["commit_hash"],
        "repo_name": cfg_file["repo_name"],
        "language": cfg_file["language"],
    }

    single_graph = {}
    for graph in cfg_file["graphs"]:
        to_hash = graph["name"] + global_template["repo_name"]
        graph_id = hashlib.md5(to_hash.encode("utf-8")).hexdigest()

        num_edges, out_degrees, in_degrees = get_graph_properties(graph["graph"])
        single_graph = {**global_template, **graph}

        single_graph["num_edges"] = num_edges
        single_graph["out_degrees"] = ",".join(out_degrees)
        single_graph["in_degrees"] = ",".join(in_degrees)

        yield graph_id, single_graph


def upload_to_elasticsearch(graph_id, graph):
    logging.info(f"PROCESS, {graph_id}")
    isomorphism_query = get_isomorphism_query(
        graph["num_edges"], graph["out_degrees"], graph["in_degrees"]
    )
    G = nx.DiGraph(graph["graph"])
    graph["graph"] = json.dumps(graph["graph"])

    if "UNREACHABLE" in graph:
        graph["UNREACHABLE"] = json.dumps(graph["UNREACHABLE"])

    if not ES.exists(index=ISOMORPHISM_INDEX, id=graph_id) and not ES.exists(
        index=CFG_INDEX, id=graph_id
    ):
        try:
            response = ES.search(
                index=CFG_INDEX, body=isomorphism_query, scroll="2m", size=SCROLL_SIZE
            )
            scroll_id = response["_scroll_id"]

            while len(response["hits"]["hits"]) > 0:
                for hit in response["hits"]["hits"]:
                    hit_id = hit["_id"]

                    hit_graph = json.loads(hit["_source"]["graph"])
                    G_potential_iso = nx.DiGraph(hit_graph)

                    if isomorphism.DiGraphMatcher(G, G_potential_iso).is_isomorphic():
                        isomorphism_data = {
                            "isomorphic_to": hit_id,
                            "filename": graph["filename"],
                            "commit_hash": graph["commit_hash"],
                            "repo_name": graph["repo_name"],
                            "language": graph["language"],
                            "name": graph["name"],
                        }
                        ES.index(
                            index=ISOMORPHISM_INDEX,
                            id=graph_id,
                            document=isomorphism_data,
                            refresh="wait_for",
                        )
                        return

                response = ES.scroll(scroll_id=scroll_id, scroll="2m")

        except NotFoundError:
            pass

        ES.index(index=CFG_INDEX, id=graph_id, document=graph, refresh="wait_for")


def run():
    for message in CONSUMER:
        data = message.value
        for graph_id, graph in get_single_cfg_will_be_loaded_to_elasticsearch(data):
            upload_to_elasticsearch(graph_id, graph)

run()
