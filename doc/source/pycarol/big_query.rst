Working with BigQuery on pyCarol
======================================

PyCarol offers a helper to work with data on Google Big Query layer, the
``pycarol.bigquery.BQ`` class. This class allows to make full requests
through the ``query()`` method, or to fetch data in chuncks through the
``paginated_query`` and ``fetch_page`` methods.

Another option for handling paginated queries is to use the 
``pycarol.bigqueryjob.BQJob`` class, which provides a way of storing
requests status and avoids passing too many parameters on methods calls.
This class also enables users to handle both paginated and full requests.

Retrieving full content at once
-------------------------------

To retrieve all the records from the query use the method ``query``, as
in the example below.

.. code:: python

   TEST_QUERY1 = """
       SELECT *
       FROM `<connector_id>.<staging_name>`
       LIMIT 1000
   """

   bq = pycarol.bigquery.BQ(login=carol_login, service_account=sa) 
   df_results = bq.query(TEST_QUERY1, return_dataframe=True)

The ``BQ`` object requires the ``Carol`` authentication, as the first
parameter, and a Google Big Query Service Account, as a second parameter.

The results can return either a list of records
(``[{”field1”: “value”, ... , ”fieldN”: “value”}, {”field1”: “value”, ... , ”fieldN”: “value”}]``)
when ``return_dataframe`` is set to ``False`` or a pandas dataframe,
when ``return_dataframe=True``.

Notice:

-  When handling high volume of data, the call may result in Out Of
   Memory errors (OOM).
-  Execution may be restricted both by Carol credentials as for Google
   service account. Make sure both have access granted to the tables
   explored on the query.

Paginated queries
-----------------

When doing pagination, the first request will always process and
generate all the results for the query. The execution is handled by a
*job* in Big Query, and the artifacts of this executions are controled
through the *Job ID*.

The results of the query are also temporary stored as artifacts, on a
*destination* table. The pagination on big query works by retrieving
data in smaller chuncks, by navigating on this temporary table.

On the example below, we setup the connection and retrieve the first 100
records from the same query used on the previous exaple.

.. code:: python

   bq = pycarol.bigquery.BQ(login=carol_login, service_account=sa) 
   df_first_page, c = bq.paginated_query(TEST_QUERY1, page_size=100, return_dataframe=True)

After initializing the BQ object, it can be used to run the query with
pagination by calling the ``paginated_query`` method together with the
``page_size`` parameter. If not defined, the ``page_size`` assumes
``500`` as default.

The method returns a tuple, composed by the first page of results, on
the first position, and a dict with control variables, on the second
position. The control variables contains the properties in the example
below:

.. code:: python

   c = {"job_id": "812ec18a-24a9-436a-ba10-004ea8bcd0a1",
   "total_records": 1000,
   "total_pages": 10),
   "current_page": 0}

The control variables can then be used to make new calls requesting
different pages of results using the ``fetch_page`` method combined with
the job id from the first request:

.. code:: python

   df_page_4, c = bq.fetch_page(job_id=c["job_id"], page_size=100, page=3, return_dataframe=True)

A few points to be considered:

-  According to big query official docs, temporary tables are stored for
   24 hours. Once it is expired, results will no longer be available for
   pagination (see `temporary
   tables <https://cloud.google.com/bigquery/docs/writing-results#:~:text=resultados%20de%20consulta.-,Tabelas%20tempor%C3%A1rias%20e%20permanentes,-O%20BigQuery%20salva>`__
   docs).
-  The last page is equal to the number of pages less one, due to zero
   indexing. An exception will be raised if the provided ``page`` is out
   of range.
-  All results are saved on the temp table on the first execution, but
   retrieved by chunks on the subsequent ``fetch_page`` (the class /
   method is memory efficient).
-  Neither Big query nor pyCarol store state for the pagination,
   therefore the parameter ``page_size`` must always be provided on
   ``paginated_query`` and ``fetch_page``, It also must be consistent
   through the calls.

Using BQJob
-----------------

As mentioned before, performing paginated queries through the BQ class requires 
the user to manage the control variables, such as the ``job_id`` and ``page_size``, 
at the client side. Another way is to work with objects of type ``BQJob``.

``BQJob`` encapsulates the whole control management, but a new object must be 
created for every new query (or whenever the query parameters are changed, such as 
``page_size``). Below is given an example of how to instantiate a ``BQJob`` object, 
as well as how to navigate on the results records.

.. code:: python

   bqj = pycarol.bigqueryjob.BQJob(carol=login, service_account=service_account,
                           query=TEST_QUERY1, page_size=200, return_dataframe=True)

   # The control variables, stored only for reference
   qcontrol = bqj.getPaginationControl()

   # Get the next page of results (the fisrt one, in this case)
   pg0 = bqj.fetch_next_page()

   # Get the second page
   pg1 = bqj.fetch_next_page()

   # Get the first page again
   pgp = bqj.fetch_previous_page()

   assert pgp.equals(pg0), "These dataframes should be equal."

   # Jumping directly to page four
   pg4 = bqj.fetch_page(page=4)

   # Retrieving all records at once
   all_records = bqj.fetch_all()

Notes:

-  Calling ``fetch_next_page`` on the last page will return the same page (last)
   no matter how many times it is called. The same goes with ``fetch_previous_page``,
   which will return always the first page.

-  Using ``fetch_page`` will update the control variables to point the provided page 
   as the last one consumed, changing the flow for ``fetch_next_page`` and 
   ``fetch_previous_page``.

-  ``fetch_all`` can be used the recover all records with a single call.

-  If not provided, ``page_size`` will default to the total of records on the results.