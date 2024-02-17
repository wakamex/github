# %%
import os

from github import Github
from dotenv import load_dotenv

load_dotenv(".env")
g = Github(os.environ["GITHUBr3"])
print(f"remaining rate limit: {g.get_rate_limit().core.remaining}")

# %%
username = "omahs"
user = g.get_user(username)

contributed_repos = user.get_repos()

repo_list = []
num_contributions = 0
for repo in contributed_repos:
    relevant_repo = repo.parent.full_name if repo.parent else repo.full_name
    print(f"{relevant_repo}")
    repo_list.append(relevant_repo)
    num_contributions += 1

print(f"{username} has contributed to {num_contributions} repositories.")
print(f"remaining rate limit: {g.get_rate_limit().core.remaining}")

# %%
with open("relevant_repos.txt","w",encoding="utf-8") as f:
    for repo in repo_list:
        f.write(f"{repo}\n")

# %%