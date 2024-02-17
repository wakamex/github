# %%
import time

import pandas as pd

from google.cloud import bigquery

# Construct a BigQuery client object
client = bigquery.Client()

# %%
# banteg query
# Define the SQL query
# query = """
#     SELECT actor.login, actor.id
#     FROM `githubarchive.year.2018`
#     WHERE actor.login IN ('rishu2403', 'tunnelpr0', 'pm-cs', 'evilebottnawi', 'wernf', 'remyabel')
#     GROUP BY 1, 2
# """
# query_job = client.query(query)
# results = query_job.result()  # Waits for the query to finish
# for row in results:
#     print(f"Login: {row.login}, ID: {row.id}")

# %%
dataset_id = 'bigquery-public-data.github_repos'
tables = client.list_tables(dataset_id)
print(f"Tables contained in '{dataset_id}':")
for table in tables:
    print(table.table_id)

# %%
start_time = time.time()
#   `bigquery-public-data.github_repos.commits`
query = """
SELECT
  *
FROM `githubarchive.month.202301`
WHERE type = 'Commit'
LIMIT 10
"""
query_job = client.query(query)
results = query_job.result()
headers = [header.name for header in results.schema]
df = results.to_dataframe()
display(df)
print(f"queried {len(df)} rows in {time.time() - start_time} seconds")

# %%
df.head(1).T

# %%
# query unique types
query = """
SELECT
  type
FROM `githubarchive.month.202301`
GROUP BY 1
"""
query_job = client.query(query)
results = query_job.result()
headers = [header.name for header in results.schema]
df = results.to_dataframe()
display(df)
print(", ".join(df.type.values))
# %%
print(", ".join(df.type.values))
# %%
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
year = 2023
strk = pd.read_parquet('strk.parquet')
users = "('" + "', '".join(strk.identity.values.tolist()) + "')"
query_job = client.query(query.format(year=year, users=users))
results = query_job.result()
headers = [header.name for header in results.schema]
df = results.to_dataframe()
df.to_parquet(f"user_repos_{year}.parquet")
display(df)

# %%
query = """
SELECT
  *
FROM
  `bigquery-public-data.github_repos.commits`
LIMIT 1
"""
query_job = client.query(query)
results = query_job.result()
headers = [header.name for header in results.schema]
df = results.to_dataframe()
display(df)
# %%
