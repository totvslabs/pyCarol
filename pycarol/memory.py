import duckdb
import pandas as pd
from typing import List, Optional, Union, Tuple
from . import exceptions


class Memory:
	def __init__(self, dfs: Optional[List[Tuple[pd.DataFrame, str]]] = None, database_path: Optional[str] = None) -> None:
		"""
		Initialize Memory class with optional database file.
		
		Args:
			dfs: List of (dataframe, table_name) tuples to cache
			database_path: Path to database file. If None, uses in-memory database.
		"""
		self.database_path = database_path
		self._is_memory_mode = database_path is None
		
		# Create read-write connection for data loading
		if self._is_memory_mode:
			self.conn = duckdb.connect(database=':memory:', read_only=False)
		else:
			self.conn = duckdb.connect(database=database_path, read_only=False)

		if dfs:
			self.cache_dataframes(dfs)

	def cache_dataframes(self, dfs: List[Tuple[pd.DataFrame, str]]) -> None:
		"""Cache multiple dataframes to the database.

		This method takes a list of (dataframe, table_name) tuples and adds each
		dataframe to the database with the specified table name.

		Args:
			dfs: List of tuples containing (dataframe, table_name) pairs to cache.

		Example:
			>>> memory = Memory()
			>>> df1 = pd.DataFrame({"a": [1, 2], "b": [3, 4]})
			>>> df2 = pd.DataFrame({"x": [5, 6], "y": [7, 8]})
			>>> memory.cache_dataframes([(df1, "table1"), (df2, "table2")])
		"""
		for df, table_name in dfs:
			self.add(table_name, df)

	def add(self, table_name: str, df: pd.DataFrame) -> None:
		"""Add or replace a table in the database.

		This method creates a new table or replaces an existing one with the
		provided dataframe. If a table with the same name already exists, it
		will be dropped and recreated with the new data.

		Args:
			table_name: Name of the table to create or replace.
			df: Pandas DataFrame containing the data to store.

		Example:
			>>> memory = Memory()
			>>> df = pd.DataFrame({"id": [1, 2], "name": ["Alice", "Bob"]})
			>>> memory.add("users", df)
		"""
		self.conn.execute(f"DROP TABLE IF EXISTS {table_name}")
		self.conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM df")

	def append(self, table_name: str, df: pd.DataFrame) -> None:
		"""Append data to an existing table in the database.

		This method adds new data to an existing table. The incoming dataframe
		will be automatically reordered to match the existing table's column
		order to ensure compatibility. The table must already exist.

		Args:
			table_name: Name of the existing table to append data to.
			df: Pandas DataFrame containing the data to append.

		Raises:
			exceptions.TableNotFoundError: If the specified table does not exist.
			exceptions.InsertOperationError: If the insert operation fails.

		Example:
			>>> memory = Memory()
			>>> # First create a table
			>>> initial_df = pd.DataFrame({"id": [1, 2], "name": ["Alice", "Bob"]})
			>>> memory.add("users", initial_df)
			>>> # Then append more data
			>>> new_df = pd.DataFrame({"id": [3, 4], "name": ["Charlie", "Diana"]})
			>>> memory.append("users", new_df)
		"""
		# Check if table exists
		try:
			table_info = self.conn.execute(f"DESCRIBE {table_name}").fetchdf()
		except Exception as e:
			raise exceptions.TableNotFoundError(f"Table '{table_name}' does not exist on Memory. Error: {str(e)}")
		
		existing_columns = table_info['column_name'].tolist()
		
		# Reorder dataframe columns to match existing table
		df_reordered = df[existing_columns]
		
		# Attempt to insert data
		try:
			self.conn.execute(f"INSERT INTO {table_name} SELECT * FROM df_reordered")
		except Exception as e:
			raise exceptions.InsertOperationError(f"Failed to insert data into table '{table_name}' on Memory. Error: {str(e)}")

	def query(self, query: str) -> pd.DataFrame:
		"""Execute a SELECT query on the database.

		This method executes SQL SELECT queries on the database. Only SELECT
		statements and Common Table Expressions (CTEs) are allowed for security.
		All queries are validated to prevent SQL injection attacks.

		Args:
			query: SQL SELECT query string to execute.

		Returns:
			pd.DataFrame: Query results as a pandas DataFrame.

		Raises:
			ValueError: If the query is not a valid SELECT statement or contains
				multiple statements with non-SELECT operations.

		Example:
			>>> memory = Memory()
			>>> memory.add("users", df)
			>>> result = memory.query("SELECT * FROM users WHERE id > 1")
			>>> result = memory.query("WITH cte AS (SELECT * FROM users) SELECT * FROM cte")
		"""
		if not self._is_select_query(query):
			raise ValueError("Only SELECT queries are allowed. Other operations are not permitted.")
		return self.conn.execute(query).fetchdf()

	def _is_select_query(self, query: str) -> bool:
		"""Check if the query contains only SELECT statements, preventing SQL injection.

		This private method validates that the provided query contains only SELECT
		statements or Common Table Expressions (CTEs). It prevents SQL injection
		by ensuring no other SQL operations (INSERT, UPDATE, DELETE, DROP, etc.)
		are present in the query.

		Args:
			query: SQL query string to validate.

		Returns:
			bool: True if the query contains only SELECT/WITH statements,
				False otherwise.
		"""
		normalized_query = query.strip()
		
		statements = [stmt.strip() for stmt in normalized_query.split(';') if stmt.strip()]
		
		for statement in statements:
			statement_upper = statement.upper()
			if not statement_upper.startswith(('SELECT', 'WITH')):
				return False
		
		return len(statements) > 0

	def is_memory_mode(self) -> bool:
		"""Check if the database is running in memory mode.

		Returns:
			bool: True if using in-memory database, False if using file database.
		"""
		return self._is_memory_mode

	def get_database_path(self) -> Optional[str]:
		"""Get the database file path.

		Returns:
			Optional[str]: Path to the database file if using file mode,
				None if using memory mode.
		"""
		return self.database_path

	def close(self) -> None:
		"""Close database connections.

		This method properly closes all database connections. It should be
		called when the Memory instance is no longer needed to free up
		resources.
		"""
		if hasattr(self, 'conn'):
			self.conn.close()

	def __enter__(self) -> 'Memory':
		"""Context manager entry.

		Returns:
			Memory: The Memory instance for use in a with statement.
		"""
		return self

	def __exit__(self, exc_type: Optional[type], exc_val: Optional[Exception], exc_tb: Optional[object]) -> None:
		"""Context manager exit.

		Automatically closes database connections when exiting the with block.

		Args:
			exc_type: Exception type if an exception occurred.
			exc_val: Exception value if an exception occurred.
			exc_tb: Exception traceback if an exception occurred.
		"""
		self.close()
