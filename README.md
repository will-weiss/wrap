Python module for handling SQL database connections, queries, and results.

### Install

<pre><code>pip install git+git://github.com/will-weiss/wrap.git</code></pre>

### Using

<pre><code>import wrap</code></pre>

### Reading from database

<pre><code>foo_db = wrap.Db(preset = 'foo', max_tries = 'debug')
ex_df = wrap.execute(
    query = "select id, bar, baz from foo.table limit 100", 
    db = foo_db, 
    return_frame = True
)</code></pre>

### Writing to database

<pre><code>wrap.write_frame(
    frame = ex_df,
    db = foo_db,
    table_name = 'foo.ex_df',
    if_exists = 'replace'
) # Currently only supports MySQL flavor</code></pre>

### Partition, Map, Reduce

<pre><code>import pandas as pd
from functools import partial

# Partition
id_range = wrap.int_range_partition(50000,60000) 

# Map
def add_multiply(query_result, num1, num2):
    return (query_result + num1) * num2

# Reduce
def concat_and_set_index(mapped_results_list, index_col):
    return pd.concat(mapped_results_list).set_index(index_col)

my_workers = wrap.Workers(5)
new_df = my_workers.execute(
    query = "select id, bar, baz from email_sends where id>={low} and id&lt;{high}",
    db = foo_db,
    partitions = id_range,
    return_frame = True,
    map_operation = partial(add_multiply, 7, 2),
    reduce_operation = partial(concat_and_set_index, index_col= 'id')
)</code></pre>