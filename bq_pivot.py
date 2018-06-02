import re

import pandas as pd


class BqPivot():
    """
    Class to generate a SQL query which creates pivoted tables in BigQuery.
    """
    def __init__(self, data, index_col, pivot_col, values_col, agg_func="sum",
                 table_name=None, not_eq_default="0", custom_agg_func=None, prefix=None, suffix=None):
        """
        blah blah
        """
        self.query = ""

        self.index_col = list(index_col)
        self.values_col = values_col
        self.pivot_col = pivot_col

        self.not_eq_default = not_eq_default
        self.table_name = self._get_table_name(table_name)

        self.piv_col_vals = self._get_piv_col_vals(data)
        self.piv_col_names = self._create_piv_col_names(pivot_col, prefix, suffix)
        
        self.function = custom_agg_func if custom_agg_func else agg_func + "({})"

    def _get_table_name(self, table_name):
        return table_name if table_name else "<--insert-table-name-here-->"

    def _get_piv_col_vals(self, data):
        """
        pass
        """
        if isinstance(data, pd.DataFrame):
            self.data = data
        elif isinstance(data, str):
            self.data = pd.read_csv(data)
        else:
            raise ValueError("Provided data must be a pandas dataframe or a csv file path.")

        return self.data[self.pivot_col].unique().tolist()
    
    def _clean_col_name(self, col_name):
        
        # replace spaces with underscores
        # remove non alpha numeric characters other than underscores
        # replace multiple consecutive underscores with one underscore
        # make all characters lower case
        return re.sub("_+", "_", re.sub('[^0-9a-zA-Z_]+', '', re.sub(" ", "_", col_name))).lower()

    def _create_piv_col_names(self, pivot_col, prefix, suffix):

        prefix = prefix + "_" if prefix else ""
        suffix = "_" + suffix if suffix else ""

        piv_col_names = ["{0}{1}_{2}{3}".format(prefix, self._clean_col_name(piv_col_val), pivot_col.lower(), suffix)
                         for piv_col_val in self.piv_col_vals]

        return piv_col_names

    def _add_select_statement(self):

        query = "select " + "".join([index_col + ", " for index_col in self.index_col]) + "\n"
        return query

    def _add_case_statement(self):
        
        case_query = self.function.format("case when {0} = \"{1}\" then {2} else {3} end") + " as {4},\n"

        query = "".join([case_query.format(self.pivot_col, piv_col_val, self.values_col,
                                           self.not_eq_default, piv_col_name)
                         for piv_col_val, piv_col_name in zip(self.piv_col_vals, self.piv_col_names)])
        
        query = query[:-2] + "\n"
        return query

    def _add_from_statement(self):

        query =  "from {0}\n".format(self.table_name)
        return query

    def _add_group_by_statement(self):

        query = "group by " + "".join(["{0},".format(x) for x in range(1, len(self.index_col) + 1)])
        return query[:-1]

    def generate_query(self):
        self.query = self._add_select_statement() +\
                     self._add_case_statement() +\
                     self._add_from_statement() +\
                     self._add_group_by_statement()

        return self.query
