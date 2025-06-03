import duckdb
import pandas as pd

# Lire depuis DuckDB
con_duckdb = duckdb.connect("veepee_ads.db")
df = con_duckdb.execute("SELECT * FROM analytics.user_affinities").df()

df