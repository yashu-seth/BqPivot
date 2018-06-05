import re

import pandas as pd


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
    def __init__(self, data, index_col, pivot_col, values_col, agg_func="sum",
                 table_name=None, not_eq_default="0", add_col_nm_suffix=True, custom_agg_func=None,
                 prefix=None, suffix=None):
        """
        Parameters
        ----------

        data: pandas.core.frame.DataFrame or string
            The input data can either be a pandas dataframe or a string path to the pandas
            data frame. The only requirement of this data is that it must have the column
            on which the pivot it to be done.

        index_col: list
            The names of the index columns in the query (the columns on which the group by needs to be performed)

        pivot_col: string
            The name of the column on which the pivot needs to be done.

        values_col: string
            The name of the column on which aggregation needs to be performed.

        agg_func: string
            The name of the sql aggregation function.

        table_name: string
            The name of the table in the query.

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
            If True, then the original pivot column name will be added as suffix in the new 
            pivoted columns.

        custom_agg_func: string, optional
            Can be used if one wants to give customized aggregation function. The values col name 
            should be replaced with {}. For example, if we want an aggregation function like - 
            sum(coalesce(values_col, 0)) then the custom_agg_func argument would be - 
            sum(coalesce({}, 0)). 
            If provided this would override the agg_func argument.

        prefix: string, optional
            A fixed string to add as a prefix in the pivoted column names separated by an
            underscore.

        suffix: string, optional
            A fixed string to add as a suffix in the pivoted column names spearated by an
            underscore.        
        """
        self.query = ""

        self.index_col = list(index_col)
        self.values_col = values_col
        self.pivot_col = pivot_col

        self.not_eq_default = not_eq_default
        self.table_name = self._get_table_name(table_name)

        self.piv_col_vals = self._get_piv_col_vals(data)
        self.piv_col_names = self._create_piv_col_names(add_col_nm_suffix, prefix, suffix)
        
        self.function = custom_agg_func if custom_agg_func else agg_func + "({})"

    def _get_table_name(self, table_name):
        """
        Returns the table name or a placeholder if the table name is not provided.
        """
        return table_name if table_name else "<--insert-table-name-here-->"

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
            raise ValueError("The provided data must have the column on which pivot is to be done. "\
                             "Also make sure that the column name in the data is same as the name "\
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

    def _create_piv_col_names(self, add_col_nm_suffix, prefix, suffix):
        """
        The method created a list of pivot column names of the new pivoted table.
        """
        prefix = prefix + "_" if prefix else ""
        suffix = "_" + suffix if suffix else ""

        if add_col_nm_suffix:
            piv_col_names = ["{0}{1}_{2}{3}".format(prefix, self._clean_col_name(piv_col_val), self.values_col.lower(), suffix)
                             for piv_col_val in self.piv_col_vals]
        else:
            piv_col_names = ["{0}{1}{2}".format(prefix, self._clean_col_name(piv_col_val), suffix)
                             for piv_col_val in self.piv_col_vals]

        return piv_col_names

    def _add_select_statement(self):
        """
        Adds the select statement part of the query.
        """
        query = "select " + "".join([index_col + ", " for index_col in self.index_col]) + "\n"
        return query

    def _add_case_statement(self):
        """
        Adds the case statement part of the query.
        """
        case_query = self.function.format("case when {0} = \"{1}\" then {2} else {3} end") + " as {4},\n"

        query = "".join([case_query.format(self.pivot_col, piv_col_val, self.values_col,
                                           self.not_eq_default, piv_col_name)
                         for piv_col_val, piv_col_name in zip(self.piv_col_vals, self.piv_col_names)])
        
        query = query[:-2] + "\n"
        return query

    def _add_from_statement(self):
        """
        Adds the from statement part of the query.
        """
        query =  "from {0}\n".format(self.table_name)
        return query

    def _add_group_by_statement(self):
        """
        Adds the group by part of the query.
        """
        query = "group by " + "".join(["{0},".format(x) for x in range(1, len(self.index_col) + 1)])
        return query[:-1]

    def generate_query(self):
        """
        Returns the query to create the pivoted table.
        """
        self.query = self._add_select_statement() +\
                     self._add_case_statement() +\
                     self._add_from_statement() +\
                     self._add_group_by_statement()

        return self.query

    def write_query(self, output_file):
        """
        Writes the query to a text file.
        """
        text_file = open(output_file, "w")
        text_file.write(self.generate_query())
        text_file.close()
