import pandas as pd

class BqPivot():
    """
    Class to generate a SQL query which creates pivoted tables in BigQuery.
    """
    def __init__(self, data, index_col, pivot_col, values_col, agg_func="sum",
                 table_name=None, custom_agg_func=None, prefix=None, suffix=None):
        """
        blah blah
        """
        self.query = ""

        self.index_col = list(index_col)
        self.values_col = values_col
        self.pivot_col = pivot_col

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

    def _create_piv_col_names(self, pivot_col, prefix, suffix):

        prefix = prefix + "_" if prefix else ""
        suffix = "_" + suffix if suffix else ""

        piv_col_names = ["{0}{1}_{2}{3}".format(prefix, piv_col_val, pivot_col.lower(), suffix)
                         for piv_col_val in self.piv_col_vals]

        return piv_col_names


