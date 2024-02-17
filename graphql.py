# %%
import json
import os
import random
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
from dotenv import load_dotenv
from requests import Session
from tqdm import tqdm

load_dotenv(".env")

session = Session()  # Create a session object

# Configuration
api_keys = [os.environ[k] for k in os.environ if k.startswith("GITHUB")]
print(f"{len(api_keys)=}")
session.keys = api_keys

rate_limit_query = """
query {
  rateLimit {
    limit
    cost
    remaining
    resetAt
  }
}
"""

def get_rate_limit(session):
    response = session.post('https://api.github.com/graphql', json={'query': rate_limit_query}, headers=headers(session))
    rate_limit_info = response.json()
    if "data" not in rate_limit_info:
        return rate_limit_info
    return rate_limit_info['data']['rateLimit']['remaining']

def check_rate_limit(session):
    print(f"remaining rate limit: {get_rate_limit(session)}")

parent_repo_query ="""
query {
  repository(owner: $owner, name: $repo) {
    name
    parent {
      name
      owner {
        login
      }
      isFork
      url
    }
  }
}
"""

def parent_repo(session, owner, repo):
    variables = {
        "owner": owner,
        "repo": repo
    }
    parent = None
    sleep_length = 2/1.25
    exponential_backoff = 1.25
    while not parent:
        try:
            response = session.post('https://api.github.com/graphql', json={'query': parent_repo_query, 'variables': variables}, headers=headers(session))
            data = response.json()
            parent = data['data']['repository']['parent']
        except Exception as exc:
            sleep_length *= exponential_backoff
            tqdm.write(f"Error fetching parent of {owner}/{repo}: {exc}, waiting for {sleep_length} seconds and trying again...")
            tqdm.write("Error message: " + response.text)
            time.sleep(sleep_length)
    return parent

query_default_branch = """
query ($owner: String!, $repo: String!) {
  repository(owner: $owner, name: $repo) {
    defaultBranchRef {
      name
    }
  }
}
"""

def fetch_default_branch_name(session, owner, repo):
    variables = {
        "owner": owner,
        "repo": repo
    }
    default_branch_name = None
    sleep_length = 2/1.25
    exponential_backoff = 1.25
    while not default_branch_name:
        try:
            response = session.post('https://api.github.com/graphql', json={'query': query_default_branch, 'variables': variables}, headers=headers(session))
            data = response.json()
            default_branch_name = data['data']['repository']['defaultBranchRef']['name']
        except Exception as exc:
            sleep_length *= exponential_backoff
            tqdm.write(f"Error fetching default branch of {owner}/{repo}: {exc}, waiting for {sleep_length} seconds and trying again...")
            tqdm.write("Error message: " + response.text)
            time.sleep(sleep_length)
    return default_branch_name

repo_commits_query = """
query ($owner: String!, $repo: String!, $branch: String!, $cursor: String, $history: Int!) {
  repository(owner: $owner, name: $repo) {
    ref(qualifiedName: $branch) {
      target {
        ... on Commit {
          history(first: $history, after: $cursor) {
            pageInfo {
              endCursor
              hasNextPage
            }
            edges {
              node {
                oid
                messageHeadline
                author {
                  name
                  email
                  user {
                    login
                  }
                }
                additions
                deletions
              }
            }
          }
        }
      }
    }
  }
}
"""

def fetch_commits(session, owner, repo, branch):
    all_commits = []
    has_next_page = True
    cursor = None
    page_num=0

    while has_next_page:
        page_num+=1
        variables = {"owner": owner,"repo": repo,"branch": branch,"cursor": cursor}
        commits = None
        sleep_length = 2/1.25
        exponential_backoff = 1.5
        history = 100
        history_divisor = 2
        while not commits:
            try:
                variables['history'] = history
                tqdm.write(f"{variables=}")
                response = session.post('https://api.github.com/graphql', json={'query': repo_commits_query, 'variables': variables}, headers=headers(session))
                data = response.json()
                commits = data['data']['repository']['ref']['target']['history']['edges']
            except Exception as exc:
                sleep_length *= exponential_backoff
                history = max(history // history_divisor, 1)
                tqdm.write(f"Error fetching page {page_num} of {owner}/{repo}: {exc}, waiting for {sleep_length} seconds and trying {history} records...")
                tqdm.write("Error message: " + response.text)
                time.sleep(sleep_length)
        all_commits.extend(commits)
        
        page_info = data['data']['repository']['ref']['target']['history']['pageInfo']
        cursor = page_info['endCursor']
        has_next_page = page_info['hasNextPage']

    return all_commits

def parse_repo(session, repo):
    owner, repo = repo.split('/')
    branch = fetch_default_branch_name(session, owner, repo)
    commits = fetch_commits(session, owner, repo, branch)
    return commits

def save_repo(session, repo):
    filepath = f"repos/{repo.replace('/', '_')}.json"
    if os.path.exists(filepath):
        print(f"{filepath} already exists, skipping...")
        return
    start_time = time.time()
    parsed_repo = parse_repo(session, repo)
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(parsed_repo, f)
    tqdm.write(f"Saved {repo} to {filepath} in {time.time() - start_time:.2f} seconds")

# Headers for the HTTP request
headers_template = {
    "Authorization": "Bearer {key}",
    "Content-Type": "application/json"
}

def headers(session):
    random_key = random.choice(session.keys)
    return {key: value.format(key=random_key) for key, value in headers_template.items()}

# %%
# parse lots of repos
os.makedirs("repos", exist_ok=True)
relevant_repos = pd.read_csv("relevant_repos.txt",header=None)[0].values
with ThreadPoolExecutor(max_workers=len(api_keys)) as executor:
    futures = [executor.submit(save_repo, session, repo) for repo in relevant_repos]
    for future in tqdm(as_completed(futures), total=len(futures)):
        result = future.result()

# %%