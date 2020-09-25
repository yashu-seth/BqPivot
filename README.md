# BqPivot

#### Google BigQuery Pivot in Python  
 
Provides a class capable of generating a pivot query using the 'Flash Pivot' implementation, which is capable of distributing work jobs to BigQuery clusters. 'Flash Pivot' provides a roughly 10x improvement in runtime compared to the more accessible style of pivot. For example, on a dataset of roughly 100mb, the original pivot took >1 minute to complete, whereas the newly implemented 'Flash Pivot' ran in 14.3 seconds. 

However, the current work would not be possible without the previous version. Credit to Yashu-Seth for implementing the original class with the traditional BigQuery Pivot. For an introduction on how that works, see [How to pivot large tables in BigQuery?](https://yashuseth.blog/2018/06/06/how-to-pivot-large-tables-in-bigquery/).

If you want to dig into the implementation of 'Flash Pivot', a guide can be found [here](https://corecompete.com/how-to-build-pivot-tables-in-bigquery-fast-and-easy/).

#### Documentation

In order to access BigQuery from Python, you will need to have a client instantiated. This requires the environment variable GOOGLE_APPLICATION_CREDENTIALS to be set to the path of your google json keyfile. From the terminal, run the command:

```
$export GOOGLE_APPLICATION_CREDENTIALS='path/to/creds.json'
```

In your Python environment, first import the google-cloud bigquery module as well as this module if it is in another directory: 

```
from google.cloud import bigquery
import BqPivot
```

Next you will need to instantiate the class. 

You must provide the name of the BigQuery table you wish to perform the pivot on, the index column to aggregate towards, the class column to pivot using, and a list of values columns to expand. 

```
gen = BqPivot(table_name='dataset.table-name',
              index_col='index',
              pivot_col='class_col',
              values_col=["value_col_1","value_col_2","value_col_3"])
```

The object will automatically query your table to retrieve a distinct list of classes from the class column. If you would rather provide the classes yourself from local data, you can do so using the `data` argument. Pass a dataframe with at least 1 column that matches the 'class_col' provided. 

In order to prevent excessive queries, the object provides multiple different query functions. 

```
gen.write_query(output_file='local/textfile.txt', verbose=True)
```

Will write out the query to a local textfile and print it out to your console. If you do not want to store a temporary file, do not pass `output_file` as an argument. If you do not want to print the query, do not pass `verbose`. 

```
df = gen.submit_pandas_query()
```

Will run the query and return the result as a pandas DataFrame using pd.read_gbq. (To do: Update to implement google-cloud native client instead, as it is now supported).

```
gen.write_permanant_table(destination_table='my-project-id:my-dataset:my-table')
```

Will submit the query to BigQuery and permanently write it to a new table indicated by `my-table`. 

Finally, the package supports command line entry. If you would like to learn more about how to use this feature, in your terminal `cd` to the directory you downloaded the package to and enter the following command:

```
$python bq_pivot.py --help
```

The script will print a list of arguments that must be passed similar to above. 

