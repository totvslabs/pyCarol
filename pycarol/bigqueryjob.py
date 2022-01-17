from .bigquery import BQ
import typing as T
import itertools
import math
from .carol import Carol
try:
    import pandas as pd
except ImportError:
    pass

class BQJob:
    """This class is used to abstract for the user all the control variables
       used for query pagination. Once the query setup is made, all the user
       have to do is call fetch_page or fetch_next_page.
    """

    def __init__(self, 
                 carol: Carol, service_account: dict, 
                 query: str, page_size: T.Optional[int] = None, dataset_id: T.Optional[str] = None, 
                 return_dataframe: bool = True
                 ):

        self.dfflag = return_dataframe

        # Creating the job for the given query
        self.bq = BQ(carol, service_account=service_account)
        self.job = self.bq.query_job(query, dataset_id)

        # Enforce query execution to make sure materialized view is created.
        self.job.result()

        # Retrieve the temp table where the results have been written to
        self.destination = self.bq.client.get_table(self.job.destination)

        # If not provided, page size defaults to the total number of records
        # recovered form the query
        if (page_size is None) or (page_size < 1):
            self.page_size = self.job.getTotalRows()
        else:
            self.page_size = page_size

        # Get iterator through chuncks of data
        page_iterator = self.bq.client.list_rows(self.destination, page_size=self.page_size)

        # Summarize control variables
        self.control = {"job_id": self.job.job_id,
                "total_records": page_iterator.total_rows,
                "total_pages": math.ceil(page_iterator.total_rows/self.page_size),
                "last_page": None}

    def fetch_all(self):
        """Fetch all records at once.

        Returns:
            returns all records on a single dataframe or list of records.

        Usage:

        .. code:: python

            import pycarol 

            bqj = pycarol.bigqueryjob.BQJob(carol=Carol(), service_account=sa,
                                            query=TEST_QUERY1, return_dataframe=True)

            all_records = bqj.fetch_all()


        """
        results = [dict(row) for row in self.job.result()]
        return pd.DataFrame(results) if self.dfflag else results

    def fetch_page(self, page: int = 0):
        """Fetch any page of results withing the range

        Args:
            page (int): The page of results to be recovered.

        Returns:
            returns a page of records on a dataframe or list of records.

        .. code:: python

            import pycarol 

            bqj = pycarol.bigqueryjob.BQJob(carol=Carol(), service_account=sa,
                                            page_size=200, query=TEST_QUERY1, return_dataframe=True)

            pg4 = bqj.fetch_page(page=4)


        """

        # Get iterator through chuncks of data
        page_iterator = self.bq.client.list_rows(self.destination, page_size=self.page_size)

        # Retrieving the given page of data from iterator
        # Next would bring the next element, using itertools
        # allows retrieving any page within the range
        p = next(itertools.islice(page_iterator.pages, page, None))

        # Assure results are stored as list of dicts
        page_results = list(p)

        # Convert to json like to dataframe if flag is set
        if self.dfflag:
            field_names = [field.name for field in page_iterator.schema]
            page_results = pd.DataFrame(data=[list(x.values()) for x in page_results], columns=field_names)

        self.control["last_page"] = page

        return page_results

    # Returns the next page after the latest one called.
    # If the final page is reached keeps returning it.
    def fetch_next_page(self):
        """Returns the next page after the latest one called.
          If the final page is reached, keeps returning it.

        Returns:
            Returns a page of records on a dataframe or list of records.

        .. code:: python

            import pycarol 

            bqj = pycarol.bigqueryjob.BQJob(carol=Carol(), service_account=sa,
                                            page_size=200, query=TEST_QUERY1, return_dataframe=True)

            pg1 = bqj.fetch_next_page()
            pg2 = bqj.fetch_next_page()


        """

        if self.control["last_page"] is None:
            self.control["last_page"] = 0

        elif self.control["last_page"] < (self.control["total_pages"] - 1):
            self.control["last_page"] += 1

        return self.fetch_page(page=self.control["last_page"])

    def fetch_previous_page(self):
        """Returns the previous page before the latest one called.
          If the first page is reached keeps returning it.

        Returns:
            Returns a page of records on a dataframe or list of records.

        .. code:: python

            import pycarol 

            bqj = pycarol.bigqueryjob.BQJob(carol=Carol(), service_account=sa,
                                            page_size=200, query=TEST_QUERY1, return_dataframe=True)

            pg1 = bqj.fetch_next_page()
            pg2 = bqj.fetch_next_page()
            pg11 = bqj.fetch_previous_page()

            assert pg11.equals(pg1), "these results will be exactly the same"


        """

        if self.control["last_page"] is None:
            self.control["last_page"] = 0

        elif self.control["last_page"] > 0:
            self.control["last_page"] -= 1

        return self.fetch_page(page=self.control["last_page"])

    def getPaginationControl(self):
        """Returns the control variables for reference.

        Returns:
            Returns a dict with job_id, total_records, total_pages and last_page keys.

        .. code:: python

            import pycarol 

            bqj = pycarol.bigqueryjob.BQJob(carol=Carol(), service_account=sa,
                                            page_size=200, query=TEST_QUERY1, return_dataframe=True)

            qc = bqj.getPaginationControl()


        """
        return self.control