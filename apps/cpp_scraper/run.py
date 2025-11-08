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

parser = argparse.ArgumentParser(description="Clang11App")

parser.add_argument("--token", type=str, help="Github Token")
parser.add_argument("--repos-num", type=int, default=1000, help="Number of considered repos")
parser.add_argument("--year", type=int, default=1000, help="Year for repo scraping")
parser.add_argument("--skip-file", type=str, default="skip_repos.txt", help="Repos that will be skipped")
args = parser.parse_args()

HEADERS = {'Authorization': f'token {args.token}'}
SKIP_FILE = args.skip_file
REPOS_NUM = args.repos_num
YEAR = int(args.year)
MAKE_FILE = "CMakeLists.txt"

def get_skip_list():
    if not os.path.exists(SKIP_FILE):
        return set()
    with open(SKIP_FILE, "r") as f:
        return set(line.strip() for line in f if line.strip())

def save_to_skip_list(repo_url):
    with open(SKIP_FILE, "a") as f:
        f.write(repo_url + "\n")

def has_repo_build_file(full_name: str, filename: str):
    url = f"https://api.github.com/repos/{full_name}/contents/{filename}"
    response = requests.get(url, headers=HEADERS)
    return response.status_code == 200

def get_date_ranges(end: str = 2017):

    while True:
        yield (f"{int(end)-1}-01-01", f"{int(end)}-01-01")
        end -= 1

def search_cpp_repos(page: int=1, per_page: int=10, date: str=""):
    url = "https://api.github.com/search/repositories"
    query = f"language:cpp created:{date[0]}..{date[1]}"
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

def build_repo(repo_name: str):
    repo_dir = repo_name.split("/")[-1]
    build_dir = os.path.join(repo_dir, "build")
    os.makedirs(build_dir, exist_ok=True)

    subprocess.run(
        ["cmake", "-S", "..", "-B", ".", "-DCMAKE_CXX_STANDARD=17", "-DCMAKE_C_STANDARD=99", "-DCMAKE_EXPORT_COMPILE_COMMANDS=ON"],
        cwd=build_dir,
        check=False
    )
    subprocess.run(
        ["cmake", "--build", "."],
        cwd=build_dir,
        check=False
    )
    
def process():
    skip_list = get_skip_list()
    logging.info("Searching top C++ repos on GitHub with build files...\n")
    per_page = 100
    repo_dir = None
    pages = REPOS_NUM // per_page
    date = None
    date_generator = get_date_ranges(YEAR)

    page_in_the_search = 1
    for page in range(pages):

        if page % 10 == 0:
            page_in_the_search = 1
            date = next(date_generator)

        for full_name, size in search_cpp_repos(page=page, per_page=100, date=date):
            repo_url = f"https://github.com/{full_name}.git"

            if repo_url in skip_list:
                logging.info(f"Skipping {repo_url} (in skip list)\n")
                continue

            logging.info(f"Checking repo: {full_name}")
            if has_repo_build_file(full_name, MAKE_FILE):

                try:
                    logging.info(f"Repository: {full_name} - Size: {size} KB")
                    logging.info(f"Cloning {repo_url} ...")

                    subprocess.run(["git", "clone", repo_url])
                    save_to_skip_list(repo_url)

                    logging.info(f"Building {full_name} ...")
                    build_repo(full_name)

                    logging.info(f"Running Clang CFG dump for {full_name} ...")
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

        page_in_the_search += 1

process()
