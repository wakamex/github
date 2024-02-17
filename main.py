# %%
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd

from github import Github
from tqdm import tqdm
from dotenv import load_dotenv

load_dotenv(".env")
g = Github(os.environ["GITHUB"])
print(f"remaining rate limit: {g.get_rate_limit().core.remaining}")

# %%
def get_unique_contributors_in_repo(g,repo):
    if isinstance(repo, str):
        repo = g.get_repo(repo)
    contributors = []
    contributions = []
    newContribs = repo.get_contributors()
    for i in newContribs:
        contributors.append(i.login)
        contributions.append(i.contributions)
        for c in i.contributions:
            print(c)
    print("unique contributors in repo:%s = %d, api calls left:%d" % (repo.full_name,len(contributors),g.get_rate_limit().core.remaining))
    return contributors,contributions

# %%
contributors,contributions = get_unique_contributors_in_repo(g,"delvtech/agent0")
contributor_df = pd.DataFrame({"contributor":contributors,"contributions":contributions})
contributor_df.loc[contributor_df.contributor == "omahs",:]

# %%
username = "omahs"
user = g.get_user(username)

contributed_repos = user.get_repos()
# walk through PaginatedList
repo_list = []
num_contributions = 0
for repo in contributed_repos:
    print(repo)
    repo_list.append(repo.full_name)
    num_contributions += 1

print(f"{username} has contributed to {num_contributions} repositories.")
print(f"remaining rate limit: {g.get_rate_limit().core.remaining}")

# %%
all_contributors = []
all_contributions = []
all_repos = []
for repo in repo_list[:50]:
    contributors,contributions = get_unique_contributors_in_repo(g,repo)
    all_contributors.extend(contributors)
    all_contributions.extend(contributions)
    all_repos.extend([repo]*len(contributors))
    print(f"parsed {repo} remaining rate limit: {g.get_rate_limit().core.remaining}")

# %%
contributors_df = pd.DataFrame({"contributor":all_contributors,"contributions":all_contributions,"repo":all_repos})
display(contributors_df.loc[contributors_df["contributor"] == "omahs",:])

# %%
repo = g.get_repo("delvtech/agent0")
c = repo.get_commits()

commit = c[0]
stats = commit.stats
stats.net = stats.additions - stats.deletions
print(f"commit {commit.sha[:5]} has total={stats.total}, additions={stats.additions} deletions={stats.deletions} net={stats.net}")
print(f"remaining rate limit: {g.get_rate_limit().core.remaining}")

# %%
def fetch_commit_data(commit):
    author = commit.author.login if commit.author else "unknown"
    if author != "unknown":
        author = author.login if "login" in author else author
    return (commit.sha, author, commit.stats.total, commit.stats.additions, commit.stats.deletions)

def get_all_commits(g, repo_name):
    repo = g.get_repo(repo_name) if isinstance(repo_name, str) else repo_name
    commits = repo.get_commits()
    total_commits = commits.totalCount

# def get_all_commits(g,repo):
#     if isinstance(repo, str):
#         repo = g.get_repo(repo)
#     commits = repo.get_commits()
#     records = []
#     for commit in tqdm(commits, total=commits.totalCount):
#         author = commit if "author" in commit else "unknown"
#         if author != "unknown":
#             author = author.login if "login" in author else author
#         record = (commit.sha,author,commit.stats.total,commit.stats.additions,commit.stats.deletions)
#         records.append(record)
#     return records

# %%
print(f"remaining rate limit: {g.get_rate_limit().core.remaining}")
commit_records = get_all_commits(g,"delvtech/agent0")
commit_df = pd.DataFrame(commit_records,columns=["sha","author","total","additions","deletions"])
print(f"remaining rate limit: {g.get_rate_limit().core.remaining}")

# %%
repo = g.get_repo("delvtech/agent0")
commits = repo.get_commits()
for d in dir(commits):
    if not d.startswith("_"):
        print(d)
# %%
