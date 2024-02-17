# %%
import os
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
from dotenv import load_dotenv
from requests import Session
from tqdm import tqdm

from graphql import save_repo

load_dotenv(".env")

session = Session()  # Create a session object

# Configuration
api_keys = [os.environ[k] for k in os.environ if k.startswith("GITHUB")]
print(f"{len(api_keys)=}")
session.keys = api_keys

strk = pd.read_parquet('strk.parquet')

# %%
# parse users
os.makedirs("repos", exist_ok=True)
relevant_repos = pd.read_csv("relevant_repos.txt",header=None)[0].values
with ThreadPoolExecutor(max_workers=len(api_keys)) as executor:
    futures = [executor.submit(save_repo, session, repo) for repo in relevant_repos]
    for future in tqdm(as_completed(futures), total=len(futures)):
        result = future.result()
    
for user in strk.identity.values:
    print(user)

# %%