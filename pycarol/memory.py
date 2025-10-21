import duckdb


class Memory:
	def __init__(self, dfs: list = None):
		self.duckdb_conn = duckdb.connect(database=':memory:', read_only=False)
		if dfs:
			self.cache_dataframes(dfs)

	def cache_dataframes(self, dfs: list):
		for df, table_name in dfs:
			self.cache_to_duckdb(table_name, df)

	def cache_to_duckdb(self, table_name, df):
		self.duckdb_conn.execute(f"DROP TABLE IF EXISTS {table_name}")
		self.duckdb_conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM df")

	def duckdb_query(self, query):
		return self.duckdb_conn.execute(query).fetchdf()