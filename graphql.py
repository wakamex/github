# %%
import json
import os
import random
import time

from tqdm import tqdm


def save_user_repos(session, username):
    user_repos = get_user_repos(session, username)
    # tqdm.write(f"Read {len(user_repos):>4} repos from {username}")
    return username, user_repos

user_repos_query = """
query($username: String!, $cursor: String, $history: Int) {
  user(login: $username) {
    repositories(first: $history, isFork: true, after: $cursor) {
      edges {
        node {
          name
          parent {
            name
            owner {
              login
            }
            url
          }
        }
      }
      pageInfo {
        endCursor
        hasNextPage
      }
    }
  }
}
"""

def get_user_repos(session, username):
    all_repos = []
    has_next_page = True
    cursor = None
    page_num=0
    while has_next_page is True:
        page_num+=1
        variables = {"username": username,"cursor": cursor}
        repos = None
        sleep_length = 2/1.25
        exponential_backoff = 1.5
        history = 100
        history_divisor = 2
        while repos is None:
            try:
                variables['history'] = history
                response = session.post('https://api.github.com/graphql', json={'query': user_repos_query, 'variables': variables}, headers=headers(session))
                data = response.json()
                repos = data['data']['user']['repositories']['edges']
                parent_repos = [repo['node']['parent'] for repo in repos if repo['node'].get('parent')]
                parent_full_names = [f"{parent['owner']['login']}/{parent['name']}" for parent in parent_repos]
                page_info = data['data']['user']['repositories']['pageInfo']
                cursor = page_info['endCursor']
                has_next_page = page_info['hasNextPage']
                all_repos.extend(parent_full_names)
                # tqdm.write(f"total repos: {len(set(all_repos))}")
            except Exception as exc:
                if "errors" in response.text and "NOT FOUND" in response.text.errors[0].type:
                    tqdm.write(f"User {username} not found")
                    return []
                sleep_length *= exponential_backoff
                history = max(history // history_divisor, 1)
                tqdm.write(f"Error fetching repos of {username}: {exc}, waiting for {sleep_length} seconds and trying {history} records...")
                tqdm.write("Error message: " + response.text)
                time.sleep(sleep_length)
    return all_repos

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
    variables = {"owner": owner,"repo": repo}
    parent = None
    sleep_length = 2/1.25
    exponential_backoff = 1.25
    while parent is None:
        try:
            response = session.post('https://api.github.com/graphql', json={'query': parent_repo_query, 'variables': variables}, headers=headers(session))
            data = response.json()
            parent = data['data']['repository']['parent']
        except Exception as exc:
            sleep_length *= exponential_backoff
            tqdm.write(f"Error fetching parent of {owner}/{repo}: {exc}, waiting for {sleep_length} seconds and trying again...")
            tqdm.write(f"Error message: {response.text}")
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
    variables = {"owner": owner,"repo": repo}
    default_branch_name = None
    sleep_length = 2/1.25
    exponential_backoff = 1.25
    while default_branch_name is None:
        try:
            response = session.post('https://api.github.com/graphql', json={'query': query_default_branch, 'variables': variables}, headers=headers(session))
            data = response.json()
            default_branch_name = data['data']['repository']['defaultBranchRef']['name']
        except Exception as exc:
            sleep_length *= exponential_backoff
            tqdm.write(f"Error fetching default branch of {owner}/{repo}: {exc}, waiting for {sleep_length} seconds and trying again...")
            tqdm.write(f"Error message: {response.text}")
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

    while has_next_page is True:
        page_num+=1
        variables = {"owner": owner,"repo": repo,"branch": branch,"cursor": cursor}
        commits = None
        sleep_length = 2/1.25
        exponential_backoff = 1.5
        history = 100
        history_divisor = 2
        while commits is None:
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
    return fetch_commits(session, owner, repo, branch)

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
