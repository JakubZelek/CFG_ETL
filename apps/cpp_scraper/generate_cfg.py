import os
import json
import subprocess
import shlex
import logging
import argparse
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
parser.add_argument(
    "--compile-commands",
    type=str,
    default="compile_commands.json",
    help="The name og the repository",
)
args = parser.parse_args()

REPO_NAME = args.repo_name
COMPILE_COMMANDS_FILENAME = args.compile_commands
COMPILE_COMMANDS_PATH = os.path.join(REPO_NAME, "build", COMPILE_COMMANDS_FILENAME)



def find_compile_command(compile_commands, filepath):
    for entry in compile_commands:
        if os.path.normpath(entry["file"]) == os.path.normpath(filepath):
            return entry["command"]
    return None


def get_commit_hash(repo_dir):
    completed_process = subprocess.run(
        ["git", "rev-parse", "HEAD"], cwd=repo_dir, capture_output=True, text=True
    )
    commit_hash = completed_process.stdout.strip()
    return commit_hash


def get_raw_cfg():
    with open(COMPILE_COMMANDS_PATH, "r", encoding="utf-8") as f:
        compile_commands = json.load(f)

    cpp_files = [entry["file"] for entry in compile_commands]

    for filename in cpp_files:
        logging.info(f"Processing file: {filename}")
        command_line = find_compile_command(compile_commands, filename)

        if not command_line:
            logging.info(f"No compile command found for {filename}")
            continue

        original_cmd_args = shlex.split(command_line)
        cmd = [
            "clang++",
            "-Xclang",
            "-analyze",
            "-Xclang",
            "-analyzer-checker=debug.DumpCFG",
            "-fsyntax-only",
        ] + original_cmd_args[1:] 

        lines = []

        process = subprocess.Popen(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True
        )
        for line in process.stdout:
            lines.append(line)
        process.wait()

        yield ".".join(filename.split("/")[-2:]), lines


def replace_keys(current_graph, current_key, value):
    current_key = current_key.upper()
    if f"({value})" in current_key:
        current_key = current_key.replace(f"({value})", "")
        current_key = current_key.strip()

        if current_key not in current_graph.get(value, {}):
            current_graph.setdefault(value, []).append(current_key)
    return current_key


def parse_cfg_to_graphs(
    lines: list[str], filename: str, commit_hash: str, repo_name: str
):
    graphs = {
        "filename": filename.split("/")[-1],
        "commit_hash": commit_hash,
        "repo_name": repo_name,
        "language": "c++11",
        "graphs": [],
    }
    header = None
    current_graph = {}

    for line in lines:
        if line and line[0].isalpha():
            if header and current_graph:
                graphs["graphs"].append(current_graph)
            header = line.replace("\n", "")
            current_graph = {}
            continue

        if line.startswith(" [B"):
            current_graph["name"] = header
            current_graph.setdefault("graph", {})

            current_key = line.split("[")[1].split("]")[0]
            current_key = replace_keys(current_graph, current_key, value="NORETURN")
            current_key = replace_keys(current_graph, current_key, value="ENTRY")
            current_key = replace_keys(current_graph, current_key, value="EXIT")

            current_graph["graph"].setdefault(current_key, [])
            continue

        if line.strip().startswith("Succs"):
            succ = line.split(":")[1].split()
            nodes = []
            for node in succ:
                node = node.upper().strip()

                if "(UNREACHABLE)" in node:
                    node = node.replace("(UNREACHABLE)", "")

                    current_graph.setdefault("UNREACHABLE", []).append(
                        {current_key: node}
                    )
                if "NULL" in node:
                    node = node.replace("NULL", "")
                    current_graph.setdefault("NULL", []).append(current_key)

                if node not in current_graph["graph"][current_key] and node != "":
                    nodes.append(node)

            current_graph["graph"][current_key].extend(nodes)
            continue

    if current_graph:
        graphs["graphs"].append(current_graph)

    if graphs["graphs"]:
        return graphs


def process_all_cfg_files():
    repo_name = REPO_NAME.split("/")[-1]
    commit_hash = get_commit_hash(repo_name)

    for filename, lines in get_raw_cfg():
        graphs = parse_cfg_to_graphs(lines, filename, commit_hash, repo_name)

        if graphs:
            PRODUCER.send(KAFKA_TOPIC, graphs)

    PRODUCER.flush()


process_all_cfg_files()
