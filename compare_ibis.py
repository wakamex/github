# %%
import os
import time

import ibis
import pandas as pd

# load results
results = pd.read_parquet("results.parquet")
results = results.drop_duplicates()

# %%
backend = "datafusion"

if backend == "duckdb":
    # Connect to DuckDB
    connection = ibis.duckdb.connect(database=':memory:')
elif backend == "pandas":
    # Connect to Pandas
    connection = ibis.pandas.connect()
elif backend == "polars":
    # Connect to Polars
    connection = ibis.polars.connect()
elif backend == "dask":
    # Connect to Dask
    connection = ibis.dask.connect()
elif backend == "datafusion":
    # Connect to DataFusion
    connection = ibis.datafusion.connect()
else:
    raise ValueError("Unsupported backend")

# %%
# Initialize a variable to hold the combined table expression
start_time = time.time()
combined_table = None
for year in range(2015, 2024):
    for part in ["first", "second"]:
        file_path = f"user_repos_{year}_{part}.parquet"
        if os.path.exists(file_path):  # Ensure the file exists
            table = connection.read_parquet(file_path)
            # add column for year
            table = table.mutate(year=year)
            if combined_table is None:
                # For the first table, just set it to the combined_table
                combined_table = table
            else:
                # Union the current table with the combined_table
                combined_table = combined_table.union(table)
load_time = time.time() - start_time
print(f"{backend}: loading took {load_time} seconds")

# %%
# de-duplicate
# Assuming `table` is your Ibis table object from any backend
start_time = time.time()
deduplicated_table = combined_table.distinct()
deduplicated_table.execute()
deduplication_time = time.time() - start_time
print(f"{backend}: de-duplication took {deduplication_time} seconds")

# %%
# do a calculation
start_time = time.time()
expr = deduplicated_table.group_by(deduplicated_table['year']).aggregate(
    count=deduplicated_table['login'].count()
)
result = expr.execute()
display(result.style.format({'count': '{:,.0f}'}))
calculation_time = time.time() - start_time
print(f"{backend}: calculation took {calculation_time} seconds")

# %%
# store results
results_pd = pd.DataFrame({
    "backend": [backend],
    "load_time": [load_time],
    "deduplication_time": [deduplication_time],
    "calculation_time": [calculation_time]
})
results = pd.concat([results, results_pd], ignore_index=True)
ibis_con = ibis.pandas.connect({'results': results})
results_table = ibis_con.table('results')
results_table.to_parquet("results.parquet")

# %%
# save to results.parquet
results.to_parquet("results.parquet")

# %%
results = pd.read_parquet("results.parquet")
results = results.drop_duplicates()
results["total"] = results["load_time"] + results["deduplication_time"] + results["calculation_time"]
results = results.sort_values(by="total", ascending=False)
display(results.style.hide(axis="index"))

 #%%