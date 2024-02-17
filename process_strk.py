# %%
import json
import pandas as pd

strk1 = json.load(open('strk1.json'))
strk2 = json.load(open('strk2.json'))

print(strk1)
print(strk2)

# %%
combined_json = strk1["eligibles"] + strk2["eligibles"]
print(f"{len(combined_json)=}")

# %%
strk = pd.DataFrame(combined_json)

# %%
strk.to_parquet('strk.parquet')

# %%
strk = pd.read_parquet('strk.parquet')
strk

# %%
