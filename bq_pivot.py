import re
import pandas as pd
import sys
from google.cloud import bigquery, storage


class BqPivot():
    """
    Class to generate a SQL query which creates pivoted tables in BigQuery.
    Example
    -------
    The following example uses the kaggle's titanic data. It can be found here -
    `https://www.kaggle.com/c/titanic/data`
    This data is only 60 KB and it has been used for a demonstration purpose.
    This module comes particularly handy with huge datasets for which we would need
    BigQuery(https://en.wikipedia.org/wiki/BigQuery).
    >>> from bq_pivot import BqPivot
    >>> import pandas as pd
    >>> data = pd.read_csv("titanic.csv").head()
    >>> gen = BqPivot(data=data, index_col=["Pclass", "Survived", "PassengenId"],
                      pivot_col="Name", values_col="Age",
                      add_col_nm_suffix=False)
    >>> print(gen.generate_query())
    select Pclass, Survived, PassengenId,
    sum(case when Name = "Braund, Mr. Owen Harris" then Age else 0 end) as braund_mr_owen_harris,
    sum(case when Name = "Cumings, Mrs. John Bradley (Florence Briggs Thayer)" then Age else 0 end) as cumings_mrs_john_bradley_florence_briggs_thayer,
    sum(case when Name = "Heikkinen, Miss. Laina" then Age else 0 end) as heikkinen_miss_laina,
    sum(case when Name = "Futrelle, Mrs. Jacques Heath (Lily May Peel)" then Age else 0 end) as futrelle_mrs_jacques_heath_lily_may_peel,
    sum(case when Name = "Allen, Mr. William Henry" then Age else 0 end) as allen_mr_william_henry
    from <--insert-table-name-here-->
    group by 1,2,3

    """

    def __init__(self, index_col, pivot_col, values_col, table_name=None, data=None,
                 agg_fun="sum", not_eq_default="0", add_col_nm_suffix=True, custom_agg_fun=None,
                 prefix=None, suffix=None):
        """
        Parameters
        ----------
        index_col: list
            The names of the index columns in the query (the columns on which the group by needs to be performed)
        pivot_col: string
            The name of the column on which the pivot needs to be done.
        values_col: string or list of strings
            The name or names of the columns on which aggregation needs to be performed.
        agg_fun: string
            The name of the sql aggregation function.
        data: pandas.core.frame.DataFrame or string
            The input data can either be a pandas dataframe or a string path to the pandas
            data frame. The only requirement of this data is that it must have the column
            on which the pivot it to be done. If data is not provided the __init__ call will
            automatically query the table_name provided to get distinct pivot column values.
            **Must provide one of data or table_name**
        table_name: string
            The name of the table in the query.
            **Must provide one of data or table_name**
        not_eq_default: numeric, optional
            The value to take when the case when statement is not satisfied. For example,
            if one is doing a sum aggregation on the value column then the not_eq_default should
            be equal to 0. Because the case statement part of the sql query would look like -

            ... ...
            sum(case when <pivot_col> = <some_pivot_col_value> then values_col else 0)
            ... ...
            Similarly if the aggregation function is min then the not_eq_default should be
            positive infinity.
        add_col_nm_suffix: boolean, optional
            If True, then the original values column name will be added as suffix in the new
            pivoted columns.
        custom_agg_fun: string, optional
            Can be used if one wants to give customized aggregation function. The values col name
            should be replaced with {}. For example, if we want an aggregation function like -
            sum(coalesce(values_col, 0)) then the custom_agg_fun argument would be -
            sum(coalesce({}, 0)).
            If provided this would override the agg_fun argument.
        prefix: string, optional
            A fixed string to add as a prefix in the pivoted column names separated by an
            underscore.
        suffix: string, optional
            A fixed string to add as a suffix in the pivoted column names separated by an
            underscore.
        """
        if data == None and table_name == None:
            raise ValueError("At least one of data or table_name must be provided.")

        self.query = ""

        self.index_col = index_col
        self.values_col = values_col
        self.pivot_col = pivot_col

        self.not_eq_default = not_eq_default

        if data is None:
            self.table_name = table_name
            self.piv_col_vals = self._query_piv_col_vals()
        elif data:
            self.piv_col_vals = self._get_piv_col_vals(data)
            self.table_name = self._get_table_name(table_name)

        if type(self.values_col) == str:
            self.piv_col_names = self._create_piv_col_names(add_col_nm_suffix, prefix, suffix)
        elif type(self.values_col) == list:
            self.piv_col_names = []
            for value_col in self.values_col:
                self.piv_col_names.append(self._create_piv_col_names(add_col_nm_suffix, prefix, suffix, value_col))

        self.ord_col_names = self._create_ord_col_names()

        self.function = custom_agg_fun if custom_agg_fun else agg_fun + "({})"

    def _get_table_name(self, table_name):
        """
        Returns the table name or a placeholder if the table name is not provided.
        """
        return table_name if table_name else "<--insert-table-name-here-->"

    def _query_piv_col_vals(self):
        '''
        Queries the distinct values in the pivot col directly from the table_name provided.
        '''
        return pd.read_gbq(f'SELECT DISTINCT({self.pivot_col}) FROM {self.table_name}')[self.pivot_col].astype(
            str).to_list()

    def _get_piv_col_vals(self, data):
        """
        Gets all the unique values of the pivot column.
        """
        if isinstance(data, pd.DataFrame):
            self.data = data
        elif isinstance(data, str):
            self.data = pd.read_csv(data)
        else:
            raise ValueError("Provided data must be a pandas dataframe or a csv file path.")

        if self.pivot_col not in self.data.columns:
            raise ValueError("The provided data must have the column on which pivot is to be done. " \
                             "Also make sure that the column name in the data is same as the name " \
                             "provided to the pivot_col parameter.")

        return self.data[self.pivot_col].astype(str).unique().tolist()

    def _clean_col_name(self, col_name):
        """
        The pivot column values can have arbitrary strings but in order to
        convert them to column names some cleaning is required. This method
        takes a string as input and returns a clean column name.
        """

        # replace spaces with underscores
        # remove non alpha numeric characters other than underscores
        # replace multiple consecutive underscores with one underscore
        # make all characters lower case
        # remove trailing underscores
        return re.sub("_+", "_", re.sub('[^0-9a-zA-Z_]+', '', re.sub(" ", "_", col_name))).lower().rstrip("_")

    def _create_piv_col_names(self, add_col_nm_suffix, prefix, suffix, value_col=None):
        """
        The method created a list of pivot column names of the new pivoted table.
        """
        prefix = prefix + "_" if prefix else ""
        suffix = "_" + suffix if suffix else ""
        if value_col == None:
            value_col = self.values_col

        if add_col_nm_suffix:
            piv_col_names = [
                "{0}{1}_{2}{3}".format(prefix, self._clean_col_name(piv_col_val), value_col.lower(), suffix)
                for piv_col_val in self.piv_col_vals]
        else:
            piv_col_names = ["{0}{1}{2}".format(prefix, self._clean_col_name(piv_col_val), suffix)
                             for piv_col_val in self.piv_col_vals]

        return piv_col_names

    def _create_ord_col_names(self):
        '''
        Create sanitized base ordinal names for each piv_col_val.
        '''

        ord_col_names = ["{}_".format(self._clean_col_name(piv_col_val))
                         for piv_col_val in self.piv_col_vals]
        return ord_col_names

    def _write_wide_ranked(self):
        '''
        Writes a 'wide_ranked' table

        To do: substitue the current piv_col_names for ordinal_piv_col_names like "ALL_" instead of "ALL_size"
        This also applies to _write_table_join in the ordinal() call
        '''

        query = 'WITH wide_ranked AS ( \nSELECT '
        query = query + "".join(["ANY_VALUE(IF({} = '{}', rank, null)) as {},\n".format(self.pivot_col,
                                                                                        pivot_col_val,
                                                                                        ord_col_name)
                                 for pivot_col_val, ord_col_name in zip(self.piv_col_vals, self.ord_col_names)])

        query = query[:-2] + '\nFROM (\nSELECT "1" AS groupby_only_col,\n'
        query = query + f"{self.pivot_col},\nRANK() over (ORDER BY {self.pivot_col}) AS rank\nFROM (\nSELECT DISTINCT {self.pivot_col}\n"
        query = query + f"FROM `{self.table_name}`\n)\n)\nGROUP BY groupby_only_col\n),\n"

        return query

    def _write_long_array_aggregated(self):
        query = ""

        # replace all self.values_col with value_col, modify table names with i
        query = query + f"long_array_aggregated AS (\n SELECT {self.index_col},\n "
        query = query + "".join(
            [f"ARRAY_AGG({values_col} ORDER BY rank) AS {values_col},\n" for values_col in self.values_col])[:-2]
        query = query + f"\nFROM (\nSELECT ranked_classes_by_id.{self.index_col} AS {self.index_col},\n"
        query = query + f"ranked_classes_by_id.rank as rank,\n"
        query = query + "".join([f"source.{values_col} as {values_col},\n" for values_col in self.values_col])[:-2]
        query = query + f"\n FROM `{self.table_name}` as source\n"
        query = query + f"RIGHT JOIN (\nSELECT {self.index_col},\n {self.pivot_col},\n"
        query = query + f"rank() over (PARTITION BY {self.index_col} ORDER BY {self.pivot_col}) as rank\n"
        query = query + f"FROM (\n SELECT DISTINCT {self.pivot_col}\n"
        query = query + f"FROM `{self.table_name}`)\n CROSS JOIN (\n SELECT DISTINCT {self.index_col}\n"
        query = query + f"FROM `{self.table_name}`)\n"
        query = query + f") as ranked_classes_by_id\n USING({self.index_col}, {self.pivot_col})\n)\nGROUP BY {self.index_col}\n)\n"

        return query

    def _write_table_join(self):

        query = f"SELECT long_array_aggregated.{self.index_col}, \n"

        for i, values_col in enumerate(self.values_col):
            query = query + "".join(
                ["Long_array_aggregated.{}[ordinal({})] as {},\n".format(values_col, ord_col_name, pivot_col_name)
                 for ord_col_name, pivot_col_name in zip(self.ord_col_names, self.piv_col_names[i])])  # values col

        # f"long_array_aggregated.{value_col}[ordinal({value for value in values_col})] as {value for value in values_col}, \n"

        query = query[:-2] + f"\nfrom "
        #         for values_col in [self.values_col]:
        query = query + "long_array_aggregated, "

        query = query + "wide_ranked"
        return query

    # replace with new query functions
    def generate_query(self):
        """
        Returns the query to create the pivoted table.

        In order to do this operation for multiple columns, we now need to iterate over the _write_long_array_aggregated function
        Or, inside long array aggregated we need to iterate over the entire block
        """
        self.query = self._write_wide_ranked() + \
                     self._write_long_array_aggregated() + \
                     self._write_table_join()

        return self.query

    def write_query(self, output_file=None, verbose=False):
        """
        Writes the query to a text file if output_file is passed, or prints the query to the console.
        """
        self.generate_query()
        if verbose:
            print(self.query)
        if output_file is not None:
            text_file = open(output_file, "w")
            text_file.write(self.generate_query())
            text_file.close()

    def submit_pandas_query(self, **kwargs):
        '''
        Submits the query and returns the results.
        '''
        if self.query == "":
            self.generate_query()
        return pd.read_gbq(self.query)

    def write_permanent_table(self, destination_table):
        job_config = bigquery.QueryJobConfig(
                                            allow_large_results=True,
                                            destination=destination_table,
                                            use_legacy_sql=True
                                                                )
        if self.query == "":
            self.generate_query()
        sql = self.query()

        # Start the query, passing in the extra configuration.
        query_job = client.query(sql, job_config=job_config)  # Make an API request.
        query_job.result()  # Wait for the job to complete.

        print("Query results loaded to the table {}".format(table_id))

    # TO DO
    #def write_temporary_table(self):


    def query_control(self, destination_table=None, local_file=None, temp_table=False):
        if local_file is not None:
            self.submit_pandas_query().to_csv(local_file)
        if destination_table is not None:
            self.write_permanant_table(destination_table)
        elif temp_table == True:
            self.write_temp_table()
        else:
            print('Final query not submitted to BigQuery. Would you like to do so now? Y/n')
            answer = input()
            if answer == 'Y' or answer == 'y':
                print('Options: \nLocal file: l\nPermanent BigQuery table: b\nTemperary BigQuery table: t')
                answer= input()
                if answer == 'l':
                    print('Local file write path: ')
                    local_file = input()
                    self.submit_pandas_query.to_csv(local_file)
                elif answer == 'b':
                    print('BigQuery table destination: ')
                    self.write_permanant_table(destination_table)
                    destination_table=None
                # elif answer == 't':
                #     temp_table=True
                #     self.write_temp_table()



if __name__ == "__main__":
    arguments = {'--output_file':None,
                 '--table_name':None,
                 '--index_col':None,
                 '--pivot_col':None,
                 '--values_col':None,
                 '--data':None,
                 '--agg_fun':"sum",
                 '--not_eq_default':"0",
                 '--add_col_nm_suffix':True,
                 '--custom_agg_fun':None,
                 '--prefix':None,
                 '--suffix':None,
                 '--verbose':False,
                 '--destination_table':None,
                 '--local_file':None,
                 '--temp_table':False}

    if '--help' in sys.argv:
        print('(Unofficial) Google BigQuery Python Pivot Script: BigPivot\n')
        print('Commands available: \n')
        print("".join([arg + ' \n' for arg in arguments]))
        print('Ex. python bq_pivot.py --index_col id --pivot_col class --values_col values --table_name my-project-id:my-dataset:my-table')
        print('\nIf you would like to run your query pass `--destination_table my-project-id:my-dataset:my-table` or `--temp_table True`')
        print('\nAlternatively, to run the query and download the result to a csv using pd.read_gbq, pass `--local_file path/to/file`')
        print('\n\nAdditionally, please make sure you set your GOOGLE_APPLICATION_CREDENTIALS using')
        print('export GOOGLE_APPLICATION_CREDENTIALS=\'path/to/creds.json\'')
        exit()

    vn = len(sys.argv)
    if vn < 9:
        raise ValueError("The following arguments are required when entering from the Command Line: \nindex_col\npivot_col\nvalues_col\ntable_name\n\
                            If you only intend to construct the query locally, pass None for table_name and the path of a data file to read.")
    for arg_name in arguments:
        if arg_name in sys.argv:
            arguments[arg_name] = sys.argv[sys.argv.index(arg_name) + 1]

    bq_client = bigquery.Client()
    storage_client = storage.Client()

    gbqPivot = BqPivot(index_col=[arguments['--index_col']],
                       pivot_col=arguments['--pivot_col'],
                       values_col=arguments['--values_col'],
                       table_name=arguments['--table_name'],
                       data=arguments['--data'],
                       agg_fun=arguments['--agg_fun'],
                       not_eq_default=arguments['--not_eq_default'],
                       add_col_nm_suffix=arguments['--add_col_nm_suffix'],
                       custom_agg_fun=arguments['--custom_agg_fun'],
                       prefix=arguments['--prefix'],
                       suffix=arguments['--suffix'])

    gbqPivot.write_query(output_file=arguments['--output_file'], verbose=arguments['--verbose'])

    gbqPivot.query_control(destination_table=arguments['--destination_table'],
                           local_file=arguments['--local_file'],
                           temp_table=arguments['--temp_table'])






