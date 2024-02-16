# %%
import pandas as pd

from github import Github


g = Github("YOURPAT")
stAPI = g.get_rate_limit().core.remaining

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
    print("unique contributors in repo:%s = %d, api calls left:%d" % (repo.organization.login+"/"+repo.name,len(contributors),g.get_rate_limit().core.remaining))
    return contributors,contributions

# %%
contributors,contributions = get_unique_contributors_in_repo(g,"delvtech/agent0")
contributor_df = pd.DataFrame({"login":contributors,"contributions":contributions})
contributor_df

# %%