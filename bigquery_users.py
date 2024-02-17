# %%
import time

import pandas as pd
from google.cloud import bigquery

# Setup BigQuery client
client = bigquery.Client.from_service_account_json("bigquerykey.json")

# %%
# prepare query
strk = pd.read_parquet('strk.parquet')
first_half = strk.identity.head(len(strk) // 2).values.tolist()
second_half = strk.identity.tail(len(strk) // 2).values.tolist()
def format_users(users):
    return "('" + "', '".join(users) + "')"

query = """
SELECT
  actor.login,
  repo.name
FROM
  `githubarchive.year.{year}`
WHERE
  type = 'PushEvent'
  AND actor.login in {users}
GROUP BY 1, 2
"""

# %%
# run query
for year in range(2015, 2024):
    try:
        start_time = time.time()
        query_job = client.query(query.format(year=year, users=format_users(first_half)))
        results = query_job.result()
        df = results.to_dataframe()
        df.to_parquet(f"user_repos_{year}_first.parquet")
        display(df)  # or print(df) if display is not available
        print(f"queried first half of {year} in {time.time() - start_time} seconds")
    except Exception as exc:
        print(f"An error occurred: {exc}")

    try:
        start_time = time.time()
        query_job = client.query(query.format(year=year, users=format_users(second_half)))
        results = query_job.result()
        df = results.to_dataframe()
        df.to_parquet(f"user_repos_{year}_second.parquet")
        display(df)  # or print(df) if display is not available
        print(f"queried second half of {year} in {time.time() - start_time} seconds")
    except Exception as exc:
        print(f"An error occurred: {exc}")

# %%
# combine all parquets
df = pd.DataFrame()
for year in range(2015, 2024):
    for part in ["first", "second"]:
        df1 = pd.read_parquet(f"user_repos_{year}_{part}.parquet")
        df = pd.concat([df, df1])

print(f"before de-duplicating: {len(df)}")
df = df.drop_duplicates()
print(f"after de-duplicating: {len(df)}")
df.to_parquet("user_repos.parquet")

# %%
# remove self-owned repos
self_owned = df.apply(lambda x: x["name"].split("/")[0] == x["login"], axis=1)
print(f"self-owned repos: {self_owned.sum()}")
df = df[~self_owned]

# %%
# common repos
common_repos = df.groupby("name").agg({"login": "count"}).reset_index().rename(columns={"login": "contributors", "name": "repo"})
common_repos = common_repos.sort_values(by="contributors", ascending=False)
common_repos.contributors.value_counts()

# %%
repos = common_repos[common_repos.contributors >= 5].repo.values
print(f"unique repos: {len(repos)}")
with open("relevant_repos.txt", "w", encoding="utf-8") as f:
    f.write("\n".join(repos)+"\n")

# %%