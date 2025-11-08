import argparse
import requests
import subprocess
import shutil
import os
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

parser = argparse.ArgumentParser(description="PythonApp")

parser.add_argument("--token", type=str, help="Github Token")
parser.add_argument("--repos-num", type=int, default=1000, help="Number of considered repos")
parser.add_argument("--year", type=int, default=2025, help="Year for repo scraping")
parser.add_argument("--skip-file", type=str, default="skip_repos.txt", help="Repos that will be skipped")
args = parser.parse_args()

HEADERS = {'Authorization': f'token {args.token}'}
SKIP_FILE = args.skip_file
REPOS_NUM = args.repos_num
YEAR = args.year

def get_skip_list():
    if not os.path.exists(SKIP_FILE):
        return set()
    with open(SKIP_FILE, "r") as f:
        return set(line.strip() for line in f if line.strip())

def save_to_skip_list(repo_url):
    with open(SKIP_FILE, "a") as f:
        f.write(repo_url + "\n")


def get_date_ranges(end: str = 2017):

    while True:
        yield (f"{int(end)-1}-01-01", f"{int(end)}-01-01")
        end -= 1

def search_python_repos(page: int=1, per_page: int=10, date: str=""):
    url = "https://api.github.com/search/repositories"
    query = f"language:python created:{date[0]}..{date[1]}"
    params = {
        "q": query,
        "sort": "stars",
        "order": "desc",
        "page": page,
        "per_page": per_page
    }
    response = requests.get(url, params=params, headers=HEADERS)
    if response.status_code == 200:
        data = response.json()
        for repo in data["items"]:
            yield repo['full_name'], repo['size']
    else:
        logging.error(f"Error: {response.status_code}, {response.text}")
        print("Error:", response.status_code, response.text)


def process():
    skip_list = get_skip_list()
    logging.info("Searching top Python repos on GitHub with build files...\n")
    repo_dir = None
    repo_number = 0
    date = None
    date_generator = get_date_ranges(YEAR)

    page = 1

    while True:

        if repo_number % 1000 == 0:
            date = next(date_generator)

        for full_name, size in search_python_repos(page=page, per_page=100, date=date):

            if repo_number == REPOS_NUM:
                return

            repo_number += 1
            repo_url = f"https://github.com/{full_name}.git"

            if repo_url in skip_list:
                logging.info(f"Skipping {repo_url} (in skip list)\n")
                continue

            logging.info(f"Checking repo: {full_name}")

            try:
                logging.info(f"Repository: {full_name} - Size: {size} KB")
                logging.info(f"Cloning {repo_url} ...")

                subprocess.run(["git", "clone", repo_url])
                save_to_skip_list(repo_url)

                logging.info(f"Generating CFGs for python for {full_name} ...")
                repo_name = full_name.split("/")[-1]
                subprocess.run(
                    ["python", "generate_cfg.py", f"--repo-name={repo_name}"],
                    check=False
                )
                repo_dir = full_name.split("/")[-1]
            except:
                pass

            if repo_dir and os.path.exists(repo_dir):
                shutil.rmtree(repo_dir, ignore_errors=True)
                logging.info(f"Removed {repo_dir} after processing.")

            else:
                logging.info(f"No recognized build files found.\n")
                save_to_skip_list(repo_url) 

        page += 1

process()
