import os
import json
import subprocess
import logging
import argparse
import types
import argparse
from typing import Any
from bytecode import Bytecode, ControlFlowGraph, BasicBlock
from kafka import KafkaProducer

KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "cfg_graphs"

PRODUCER = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)

parser = argparse.ArgumentParser(description="CppApp")
parser.add_argument("--repo-name", type=str, help="The name og the repository")
args = parser.parse_args()

REPO_NAME = args.repo_name


def get_python_files(repo_path):
    for root, _, files in os.walk(repo_path):
        for f in files:
            if f.endswith(".py"):
                yield os.path.join(root, f)

def get_cfg_dict(blocks: ControlFlowGraph) -> dict[str, list[str]]:
    cfg_graph: dict[str, list[str]] = {}

    for block in blocks:
        block_index = blocks.get_block_index(block)
        block_id = f"B{block_index}"
        cfg_graph[block_id] = []

        for instr in block:
            outgoing_blocks = []

            if hasattr(instr, "arg") and isinstance(instr.arg, BasicBlock):
                outgoing_blocks.append(instr.arg)

            if hasattr(instr, "target") and isinstance(instr.target, BasicBlock):
                outgoing_blocks.append(instr.target)

            if hasattr(instr, "handlers") and isinstance(instr.handlers, list):
                for handler_block in instr.handlers:
                    if isinstance(handler_block, BasicBlock):
                        outgoing_blocks.append(handler_block)

            for out_block in outgoing_blocks:
                out_block_index = blocks.get_block_index(out_block)
                outgoing_block_id = f"B{out_block_index}"
                if outgoing_block_id not in cfg_graph[block_id]:
                    cfg_graph[block_id].append(outgoing_block_id)

        if block.next_block is not None:
            next_block_index = blocks.get_block_index(block.next_block)
            next_block_id = f"B{next_block_index}"
            if next_block_id not in cfg_graph[block_id]:
                cfg_graph[block_id].append(next_block_id)

    return cfg_graph


def extract_code_objects_with_context(code_obj: types.CodeType, parent_name: str = "") -> list[dict[str, Any]]:
    objects = []
    qualified_name = f"{parent_name}.{code_obj.co_name}" if parent_name else code_obj.co_name
    objects.append({"name": qualified_name, "code": code_obj})

    for const in code_obj.co_consts:
        if isinstance(const, types.CodeType):
            objects.extend(extract_code_objects_with_context(const, parent_name=qualified_name))
    return objects

def filter_reachable_blocks(graph: dict[str, list[str]], start: str = "B0") -> dict[str, list[str]]:
    """
    Returns just the blocks, that are reachable from start.
    """
    reachable = []
    stack = [start]

    while stack:
        node = stack.pop()
        if node not in reachable:
            reachable.append(node)
            stack.extend(graph.get(node, []))

    return {node: graph[node] for node in reachable}

def parse_cfg_to_graphs(
    module_code: types.CodeType,
    filename: str = None,
    commit_hash: str = None,
    repo_name: str = None,
):
    graphs = {
        "filename": filename.split("/")[-1],
        "commit_hash": commit_hash,
        "repo_name": repo_name,
        "language": "python",
        "graphs": [],
    }

    all_code_objects = extract_code_objects_with_context(module_code)
    for obj in all_code_objects:
        code_obj = obj["code"]

        if code_obj.co_name != "<module>":
            name = obj["name"].replace("<module>.", "")
            bytecode = Bytecode.from_code(code_obj)
            cfg = ControlFlowGraph.from_bytecode(bytecode)
            graph = get_cfg_dict(cfg)
            graph = filter_reachable_blocks(graph)
            if len(graph) > 1:

                exit_nodes = ",".join(sorted([node_id for node_id in graph if len(graph[node_id]) == 0]))
                entry_nodes = set()
                for outgoing_nodes in graph.values():
                    entry_nodes |= set(outgoing_nodes)
                entry_nodes = set(graph) - entry_nodes
                entry_nodes = ",".join(sorted(list(entry_nodes)))
                current_graph = {"name": name,
                                 "graph": graph,
                                 "ENTRY": entry_nodes,
                                 "EXIT": exit_nodes}
                
                graphs["graphs"].append(current_graph)

    return graphs


def get_commit_hash(repo_dir):
    completed_process = subprocess.run(
        ["git", "rev-parse", "HEAD"], cwd=repo_dir, capture_output=True, text=True
    )
    commit_hash = completed_process.stdout.strip()
    return commit_hash


def process_all_cfg_files():
    repo_name = REPO_NAME.split("/")[-1]
    commit_hash = get_commit_hash(repo_name)
    for file_path in get_python_files(repo_name):
        with open(file_path, "r") as f:
            source = f.read()
        module_code = compile(source, file_path, "exec")
        graphs = parse_cfg_to_graphs(
            module_code,
            filename=file_path,
            commit_hash=commit_hash,
            repo_name=repo_name,
        )
        if graphs:
            PRODUCER.send(KAFKA_TOPIC, graphs)

    PRODUCER.flush()

process_all_cfg_files()