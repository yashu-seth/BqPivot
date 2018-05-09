import pandas as pd

class BqPivot():
    """
    Class to generate a SQL query which creates pivoted tables in BigQuery.
    """
    def __init__(self, data, index_col, pivot_col, values_col, agg_func="sum",
                 custom_agg_func=None, prefix=None, suffix=None):
        """
        blah blah
        """
        self.query = ""

        if isinstance(data, pd.DataFrame):
            self.piv_col_names = self.create_piv_col_names(data, pivot_col, prefix, suffix)
        elif isinstance(data, str):
            self.piv_col_names = self.create_piv_col_names(pd.read_csv(data), pivot_col, prefix, suffix)
        else:
            raise ValueError("Provided data must be a pandas dataframe or a csv file path.")

        self.index_col = list(index_col)
        self.values_col = values_col

        self.function = custom_agg_func if custom_agg_func else agg_func + "({})"


    def create_piv_col_names(self, pivot_col, prefix, suffix):
        pass
