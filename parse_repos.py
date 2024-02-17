# %%
import json
import os

import pandas as pd

# %%
# parse json files in /repo
repos = os.listdir("repos")
print(f"parsing {len(repos)} repos")

person_of_interest = "omahs"
max_repo_name_length = max(len(repo.replace('.json','')) for repo in repos)
records = []
for repo in repos:
    if f"{person_of_interest}_" in repo:
        continue
    with open(f"repos/{repo}", encoding="utf-8") as f:
        commits = json.load(f)
    filtered_commits = []
    for commit in commits:
        if commit is None or "node" not in commit:
            continue
        node = commit['node']
        if node is None or "author" not in node:
            continue
        author = node['author']
        if author is None or "user" not in author:
            continue
        user = author['user']
        if user is None or "login" not in user:
            continue
        login = user['login']
        if login == person_of_interest:
            filtered_commits.append(node)
            print(f"{repo.replace('.json',''):<{max_repo_name_length}}:{node['oid'][:7]} +{node['additions']}-{node['deletions']} {node['messageHeadline']}")
            records.append({
                "repo": repo.replace('.json',''),
                "message": node['messageHeadline'],
                "total": node['additions'] + node['deletions'],
                "additions": node['additions'],
                "deletions": node['deletions'],
                "net": node['additions']-node['deletions'],
                "author": login,
                "oid": node['oid']
            })

# %%
# function to filter commits
def drop_commits(_df, idx, name):
    print(f"filtering out {idx.sum()} {name} commits")
    prev_repos = _df.repo.unique()
    _df = _df[~idx]
    print("remaining commits:",len(_df))
    print(f"unique repos:{_df.repo.nunique()}: {', '.join(_df.repo.unique())}")
    print(f"dropped: {', '.join(set(prev_repos) - set(_df.repo.unique()))}")
    return _df

# %%
all_commits = pd.DataFrame(records)
print("all:",len(all_commits))
print(f"unique repos:{all_commits.repo.nunique()}: {', '.join(all_commits.repo.unique())}")

# %%
# filter out merge commits
df = drop_commits(all_commits, all_commits.message.str.contains("Merge branch"), "merge")

# %%
# filter out typo commits
df = drop_commits(df, df.message.str.contains("typo") | df.message.str.contains("Typo"), "typo")

# %%
# filter out small commits
df = drop_commits(df, (abs(df.net) < 10) | (df.additions < 10) & (df.deletions < 10), "small")

# %%
# filter out additions only
df = drop_commits(df, df.deletions == 0, "additions only")

# %%
# remaining
display(df.sort_values(by="additions", ascending=False).style.hide(axis="index").hide(subset="oid",axis="columns"))
display(', '.join(df.oid.values))

# %%
