# %%
import os
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
from dotenv import load_dotenv
from requests import Session
from tqdm import tqdm

from graphql import save_user_repos

load_dotenv(".env")

session = Session()  # Create a session object

# Configuration
api_keys = [os.environ[k] for k in os.environ if k.startswith("GITHUB")]
print(f"{len(api_keys)=}")
session.keys = api_keys

strk = pd.read_parquet('strk.parquet')
parsed_usernames = pd.read_csv("parsed_usernames.csv", header=None)[0].values if os.path.exists("parsed_usernames.csv") else []

# %%
os.makedirs("users", exist_ok=True)
with ThreadPoolExecutor(max_workers=len(api_keys)) as executor:
    futures = [executor.submit(save_user_repos, session, user) for user in strk.identity.values if user not in parsed_usernames]
    for future in tqdm(as_completed(futures), total=len(futures)):
        username, user_repos = future.result()
        if len(user_repos) == 0:
            user_repos = ["!none"]
        with open("parsed_usernames.csv", "a", encoding="utf-8") as f:
            f.write(f"{username}\n")
        with open(f"users/{username}.csv", "w", encoding="utf-8") as f:
            f.write("\n".join(user_repos)+"\n")

# %%
parsed_usernames = pd.read_csv("parsed_usernames.csv", header=None)[0].values if os.path.exists("parsed_usernames.csv") else []
print(f"{len(parsed_usernames)=}")

# %%