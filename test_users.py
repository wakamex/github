# %%
import os

import pandas as pd
from dotenv import load_dotenv
from requests import Session

from graphql import get_user_repos

load_dotenv(".env")

session = Session()  # Create a session object

# Configuration
api_keys = [os.environ[k] for k in os.environ if k.startswith("GITHUB")]
print(f"{len(api_keys)=}")
session.keys = api_keys

# %%
username = "meyusufdemirci"
user_repos = get_user_repos(session, username)

# %%