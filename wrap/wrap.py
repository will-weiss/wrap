### A set of methods for utilizing multiple workers in connecting to, reading from, and writing to sql databases
import datetime
import time
import math
import MySQLdb
import MySQLdb.cursors
import pandas as pd
import pandas.io.sql as psql
import multiprocessing as mp
import copy
import itertools
import psycopg2 as psg
from numpy.random import rand
import hashlib
import re
from impala.dbapi import connect as imp_connect
from impala.util import as_pandas
import functools
from functools import partial

PRESETS = {
    'example' : {
        'basic_kwargs' : {
            'host'      : 'hostname',
            'port'      : 3306,
            'user'      : 'username',
            'password'  : 'password',
            'db'    : 'dbname',
        },
        'additional_kwargs' : None,
        'flavor'    : 'mysql',
    },
}

FLAVOR_INFO = {
    'names' : {
        'host' : {
            'mysql' : 'host',
            'pg'    : 'host',
            'imp'   : 'host',
        },
        'port' : {
            'mysql' : 'port',
            'pg'    : 'port',
            'imp'   : 'port',
        },
        'user' : {
            'mysql' : 'user',
            'pg'    : 'user',
            'imp'   : 'user',
        },
        'password' : {
            'mysql' : 'passwd',
            'pg'    : 'password',
            'imp'   : 'password',
        },
        'db' : {
            'mysql' : 'db',
            'pg'    : 'database',
            'imp'   : 'db',
        },
    },
    'connect_method' : {
        'mysql' : MySQLdb.connect,
        'pg'    : psg.connect,
        'imp'   : imp_connect,
    },
    'other_ts_sql' : {
        'mysql' : "select unix_timestamp('{0}')",
        'pg'    : "select extract(epoch from '{0}'::timestamp)",
        'imp'   : "select unix_timestamp('{0}')",
    },
    'now_ts_sql' : {
        'mysql' : "select unix_timestamp()",
        'pg'    : "select extract(epoch from now())",
        'imp'   : "select unix_timestamp()",
    },
    'single_val_sql' : {
        'mysql' : "select {0} from {1} {2} and {0} >= {3} and {0} < {4} limit 1",
        'pg'    : "select {0} from {1} {2} and {0} >= {3} and {0} < {4} limit 1",
    },
    'int_range_sql' : {
        'mysql' : "select min({0}), max({0}) + 1 from {1} {2}",
        'pg'    : "select min({0}), max({0}) + 1 from {1} {2}",
    },
    'iter_sql' : {
        'mysql' : "select distinct {0} as {0} from {1} {2}",
        'pg'    : "select distinct {0} as {0} from {1} {2}",
    },
    'replace_rows_sql' : {
        'mysql' : "replace into {0} select * from {1}",
    },
    'insert_ignore_sql' : {
        'mysql' : "insert ignore into {0} select * from {1}",
    },
    'drop_dummy_sql' : {
        'mysql' : "drop table {0}",
    },
}

def connect(host,port,user,password,db,additional_kwargs,flavor,wait,max_tries):

    basic_kwargs = _gen_basic_kwargs(host,port,user,password,db)
    kwargs = _gen_formatted_kwargs(basic_kwargs,additional_kwargs,flavor)
    return _connect_with_kwargs(kwargs,flavor,wait,max_tries)

def execute(
    query,
    db,
    fetch = False,
    commit = False,
    return_frame = False,
    index_col = None,
    coerce_float = False,
    params = None,
    conn = None,
    cursor = None,
    return_conn_objs = False
):
    _check_execute_args(query, db, fetch, commit, return_frame)
    return _perform_operation(
        method = partial(_attempt_execute, query = query, db = db, fetch = fetch, commit = commit, return_frame = return_frame, index_col = index_col, coerce_float = coerce_float, params = params, conn = conn, cursor = cursor, return_conn_objs = return_conn_objs),
        max_tries = db.max_tries,
        wait = db.wait
    )

def execute_frame(
    query,
    frame,
    db,
    fetch = False,
    commit = False,
    return_frame = False,
    index_col = None,
    coerce_float = False,
    params = None,
    concat_results = True,
    conn = None,
    cursor = None,
    return_conn_objs = False
):
    _check_execute_args(query, db, fetch, commit, return_frame)
    
    if conn is None: conn, cursor = db.get_conn_objs()

    data_parts = []
    for _, row in frame.iterrows():
        conn, cursor, data_part = _perform_operation(
            method = partial(_attempt_execute, query = query.format(**row.to_dict()), db = db, fetch = fetch, commit = commit, return_frame = return_frame, index_col = index_col, coerce_float = coerce_float, params = params, conn = conn, cursor = cursor, return_conn_objs = True),
            max_tries = db.max_tries,
            wait = db.wait,
        )
        if data_part is not None: data_parts.append(data_part)
            
    if data_parts == []:
        data = None
    elif concat_results:
        if return_frame:
            data = pd.concat(data_parts)
            if index_col is None:
                data = data.reset_index(drop = True)
        else:
            data = tuple(itertools.chain(*data_parts))
    else:
        data = data_parts

    if return_conn_objs: return conn, cursor, data
    _close_all(conn, cursor)
    return data

def write_frame(
    frame, 
    db, 
    table_name, 
    if_exists = 'fail', 
    convert_nan = True, 
    index = False, 
    index_label = False,
    conn = None,
    cursor = None,
    return_conn_objs = False
):
    if frame is None: return
    if convert_nan: frame = frame.where((pd.notnull(frame)), None)   
    if db.flavor != 'mysql': raise RuntimeError("Currently wrap only supports writing dataframes to mysql databases.")
    if if_exists not in set(['append','replace','fail','insert_ignore','replace_rows']): raise RuntimeError("if_exists has unrecognized value: " + if_exists + "\nif_exists can only take values 'append', 'replace', 'fail', 'insert_ignore', and 'replace_rows'")

    if (if_exists=='replace_rows') | (if_exists=='insert_ignore'):
        create_table_query = _get_create_table_query(db, table_name)
        if create_table_query is not None:
            dummy_name = _gen_dummy_table(db, frame, create_table_query, index, index_label)
            _utilize_dummy(db, table_name, dummy_name, if_exists)
            _drop_dummy(db, dummy_name, is_error = False)
            return
        else:
            if_exists = 'fail'
    
    return _perform_operation(
        method = partial(_write_frame, frame = frame, db = db, table_name = table_name, if_exists = if_exists, index = index, index_label = index_label, conn = conn, cursor = cursor, return_conn_objs = return_conn_objs),        
        max_tries = db.max_tries,
        wait = db.wait
    )

class int_range_partition(object):
    def __init__(self, low_val, high_val, low_placeholder = 'low', high_placeholder = 'high', num = 100):
        self.low_val = int(low_val) if isinstance(low_val, (int, float, long)) else 0
        self.high_val = int(math.ceil(high_val)) if isinstance(high_val, (int, float, long)) else 0
        self.low_placeholder = low_placeholder
        self.high_placeholder = high_placeholder
        self.num = int(math.ceil(num))
        self.format_dict_list = [{
                                    self.low_placeholder    : (self.low_val + int((self.high_val - self.low_val) * (float(i) / float(self.num)))),
                                    self.high_placeholder   : (self.low_val + int((self.high_val - self.low_val) * (float((i+1)) / float(self.num))))
                                } for i in xrange(self.num)]

class iter_partition(object):
    def __init__(self, checklist, placeholder = 'check'):
        self.checklist = checklist
        self.placeholder = placeholder
        self.format_dict_list = [{self.placeholder : check_val} for check_val in self.checklist]

class hex_char_partition(object):
    def __init__(self, placeholder = 'hex', num_chars=2):
        self.placeholder = placeholder
        self.num_chars = num_chars
        self.format_dict_list = self._gen_format_dict_list()

    def _gen_format_dict_list(self):
        HEX_CHARS = ['0','1','2','3','4','5','6','7','8','9','a','b','c','d','e','f']
        hex_char_lists = [HEX_CHARS for _ in xrange(self.num_chars)]
        hex_chars_list_spliced = list(itertools.product(*hex_char_lists))
        format_dict_list = []
        for hex_chars in hex_chars_list_spliced:
            format_dict_list.append({self.placeholder : ''.join(hex_chars)})
        return format_dict_list

class from_dict_list_partition(object):
    def __init__(self, dict_list):
        self.format_dict_list = [dict_list] if not isinstance(dict_list, (tuple, set, list)) else list(dict_list)

class Db(object):
    def __init__(
        self,
        preset = None,
        host = None,
        port = None,
        user = None,
        password = None,
        db = None,
        additional_kwargs = None,
        flavor = None,
        wait = 1,
        max_tries = 5,
    ):
        self.preset = preset

        if self.preset is not None:
            self.flavor = PRESETS[preset]['flavor'].lower()
            self.kwargs = _gen_formatted_kwargs(PRESETS[preset]['basic_kwargs'], PRESETS[preset]['additional_kwargs'], self.flavor)
        else:
            self.kwargs = {}

        if flavor is not None: self.flavor = flavor.lower()

        basic_kwargs = _gen_basic_kwargs(host,port,user,password,db)
        new_kwargs = _gen_formatted_kwargs(basic_kwargs, additional_kwargs, self.flavor)

        for k, v in new_kwargs.iteritems():
            if v is not None:
                self.kwargs[k] = v

        self.wait = float(wait)
        self.max_tries = max_tries.lower() if type(max_tries) == str else int(max_tries)

    def get_conn_objs(self):
        return _connect_with_kwargs(self.kwargs,self.flavor,self.wait,self.max_tries)

    def ts(self, day=None):
        use_sql = FLAVOR_INFO['now_ts_sql'][self.flavor] if day is None else FLAVOR_INFO['other_ts_sql'][self.flavor].format(day.strftime('%Y-%m-%d')) if isinstance(day, (datetime.date,datetime.datetime)) else FLAVOR_INFO['other_ts_sql'][self.flavor].format(day) if isinstance(day, (str)) else None
        if use_sql is None: raise RuntimeError("get_ts takes datetime.datetime or datetime.date objects only.")
        return int(execute(query = use_sql, db = self, fetch = True)[0][0])

    def gen_iter_partition(self, table_name, col_name, placeholder='check', restriction_clause = None):
        if restriction_clause is None: restriction_clause = ""
        use_iter_sql = FLAVOR_INFO['iter_sql'][self.flavor].format(col_name, table_name, restriction_clause)
        checklist = execute(query = use_iter_sql, db = self, return_frame = True).loc[:,col_name].tolist()
        return iter_partition(checklist, placeholder)

    def gen_int_range_partition(self, table_name, col_name, low_placeholder="low", high_placeholder="high", num = 100, restriction_clause = None, exact = True):
        if exact | (restriction_clause is None):
            if restriction_clause is None: restriction_clause = ""
            int_range = execute(query = FLAVOR_INFO['int_range_sql'][self.flavor].format(col_name, table_name, restriction_clause), db = self, fetch = True)[0]
            return int_range_partition(int_range[0], int_range[1], low_placeholder, high_placeholder, num)
        else:
            use_int_range = execute(query = FLAVOR_INFO['int_range_sql'][self.flavor].format(col_name, table_name, ''), db = self, fetch = True)[0]
            use_int_range = _search_bound(use_int_range, self, table_name, col_name, restriction_clause, get_max = True)
            use_int_range = _search_bound(use_int_range, self, table_name, col_name, restriction_clause, get_max = False)

def _search_bound(use_int_range, db, table_name, col_name, restriction_clause, get_max = True, block_perc = .01):
    
    val_ret = None
    block_size = int(block_perc * (use_int_range[1] - use_int_range[0]))

    while (val_ret is None) & (block_size <= use_int_range[1] - use_int_range[0]):
        if get_max  : block = (use_int_range[1] - block_size, use_int_range[1])
        else        : block = (use_int_range[0], use_int_range[0] + block_size)
        
        parent_conn, child_conn = mp.Pipe()

        p = mp.Process(
            target = _piped_func, 
            kwargs= {
                'child_conn' : child_conn,
                'use_func' : partial(execute, 
                    query = FLAVOR_INFO['single_val_sql'][db.flavor].format(col_name, table_name, restriction_clause, block[0], block[1]),
                    db    = db,
                    fetch = True
                )
            },
        )
        p.start()
        time.sleep(.2)
        if p.is_alive():
            p.terminate()
            if get_max  : use_int_range = (use_int_range[0] , block[1])
            else        : use_int_range = (block[0] , use_int_range[1])
        else:
            val_ret = parent_conn.recv()

    return use_int_range

class Workers(object):
    def __init__(self, number = 1):
        self.number = number

    def _map_execute(self, worker_feed):
        pool = mp.Pool(self.number)
        mapped_result_list = pool.map(_expl_map_execute,worker_feed)
        pool.close()
        pool.join()
        return mapped_result_list

    def execute(
        self, 
        query,
        partitions,
        db,
        fetch = False,
        commit = False,
        return_frame = False,
        index_col = None,
        coerce_float = False,
        params = None,
        map_operation = None,
        reduce_operation = None
    ):
        map_operation = _return_given if map_operation is None else map_operation
        reduce_operation = _return_given if reduce_operation is None else reduce_operation

        worker_feed = [[query.format(**whole_format_dict), db, fetch, commit, return_frame, index_col, coerce_float, params, map_operation] for whole_format_dict in _gen_whole_format_dict_list(partitions)]
        if len(worker_feed) == 0: return

        mapped_result_list = [_expl_map_execute(feed) for feed in worker_feed] if self.number == 1 else self._map_execute(worker_feed)
        return _attempt_method(reduce_operation, mapped_result_list)

#############################################
### PRIVATE FUNCTIONS
#############################################

# Sends the results of a function through a pipe
def _piped_func(child_conn, use_func):
    child_conn.send(use_func())
    child_conn.close()

# Returns a conn and cursor object from connection information
def _connect(kwargs,flavor):
    conn_func = FLAVOR_INFO['connect_method'][flavor]
    conn = conn_func(**kwargs)
    cursor = conn.cursor()
    return conn, cursor

# Attempts query execution
def _attempt_execute(
    query,
    db,
    fetch,
    commit,
    return_frame,
    index_col,
    coerce_float,
    params,
    conn = None,
    cursor = None,
    return_conn_objs = False,
):
    if db.max_tries == 'debug':
        print_query = str(query) if len(query) <= 200 else str(query)[:97] + " ... " + str(query)[-97:]
        print "EXECUTING <" + print_query + ";>..."

    if conn is None: conn, cursor = db.get_conn_objs()
    
    data = None

    if return_frame:
        if db.flavor in ('mysql','pg'):
            data = psql.read_sql(query, con=conn, index_col=index_col, coerce_float=coerce_float, params=params)
        else:
            cursor.execute(query)
            data = as_pandas(cursor)
    else:
        cursor.execute(query)
        if commit:
            conn.commit()
        elif fetch:
            data = cursor.fetchall()

    if not return_conn_objs:
        _close_all(conn, cursor)
        return data

    if data is None: return conn, cursor
    return conn, cursor, data

# Checks to see if arguments passed to execute statement make sense
def _check_execute_args(query, db, fetch, commit, return_frame):
    if query is None: raise RuntimeError("Query required!")
    if db is None: raise RuntimeError("A wrap.Db object to read from is required!")
    if sum([fetch, commit, return_frame]) > 1: raise RuntimeError("The result of the query can only be one of the following: fetched as a tuple of tuples, committed, or returned as a dataframe.")

# Returns functools.partial from a function or functools.partial
def _partialize_method(method):
    return method if isinstance(method, functools.partial) else partial(method)

# Attempts a method in method_info. Argument order is x, args, then kwargs.
def _attempt_method(method, x = None):
    if x is None: return method()

    method = _partialize_method(method)
    new_args = tuple([x]) + method.args
    new_keywords = {} if method.keywords is None else method.keywords
    method = partial(method.func, *new_args, **new_keywords)

    return method()

# Performs an operation
def _perform_operation(method, max_tries, wait, x = None, on_error = None, attempt_kwargs = None, max_attempts = None):
    
    method = _partialize_method(method)
    max_attempts = max_attempts if max_attempts is not None else 1 if max_tries == 'debug' else max_tries
    
    while max_attempts > 0:
        new_keywords = None if attempt_kwargs is None else attempt_kwargs.pop(0) if method.keywords is None else dict(method.keywords.items() + attempt_kwargs.pop(0).items())
        method = method if new_keywords is None else partial(method.func, *method.args, **new_keywords)
        if (max_attempts == 1) & (on_error is None): return _attempt_method(method, x)
        try:
            return _attempt_method(method, x)
        except:
            max_attempts += -1
            time.sleep(wait)
    return _attempt_method(on_error)
            
# Replaces or inserts data in an existing table with data from a dummy table
def _utilize_dummy(db, table_name, dummy_name, if_exists):
    
    use_sql = FLAVOR_INFO['replace_rows_sql'][db.flavor].format(table_name, dummy_name) if if_exists == 'replace_rows' else FLAVOR_INFO['insert_ignore_sql'][db.flavor].format(table_name, dummy_name) if if_exists == 'insert_ignore' else None
    if use_sql is None: raise RuntimeError("Attempting to utilize dummy for unrecognized if_exists value: " + if_exists)
    try:
        execute(query = use_sql, db = db, commit = True)
    except:
        _drop_dummy(db, dummy_name, is_error = True)

# Tests whether a table with the dummy_name exists in the database and raises an error if so.
def _test_dummy_name(dummy_name, db):
    conn, cursor = db.get_conn_objs()
    if psql.table_exists(dummy_name, conn, db.flavor):
        _close_all(conn, cursor)
        raise RuntimeError("table with name " + dummy_name + " already exists!")
    _close_all(conn, cursor)
    return dummy_name

# Generates dummy table names
def _gen_dummy_table(db, frame, create_table_query, index, index_label):

    dummy_names = ["____" + new_seed for new_seed in _gen_new_random_seed_set(1 if db.max_tries == 'debug' else db.max_tries)]
    dummy_name = _perform_operation(
        method = partial(_test_dummy_name, db = db),
        max_tries = db.max_tries,
        wait = db.wait,
        attempt_kwargs = [{'dummy_name': dummy_name} for dummy_name in dummy_names]
    )
    
    execute(query = create_table_query.format(**{'DUMMY_NAME':dummy_name}), db = db, commit = True)
    
    _perform_operation(
        method = partial(_write_frame, frame = frame, db = db, table_name = dummy_name, if_exists = 'append', index = index, index_label = index_label),
        max_tries = db.max_tries,
        wait = db.wait,
        on_error = partial(_drop_dummy, db = db, dummy_name = dummy_name, is_error = True)
    )
    
    return dummy_name

# Writes a table to a database for one of the native if_exists options in pandas.io.sql 
def _write_frame(
    frame, 
    db, 
    table_name, 
    if_exists, 
    index, 
    index_label, 
    conn = None, 
    cursor = None,
    return_conn_objs = False
):
    if db.max_tries == 'debug': print "WRITING DATAFRAME ..."
    if conn is None: conn, cursor = db.get_conn_objs()
    
    psql.to_sql(frame = frame, name = table_name, con = conn, flavor = 'mysql', if_exists = if_exists, index = index, index_label = index_label)

    if return_conn_objs: return conn, cursor
    _close_all(conn, cursor)

# Drops a dummy table
def _drop_dummy(db, dummy_name, is_error = False):
    execute(query = FLAVOR_INFO['drop_dummy_sql'][db.flavor].format(dummy_name), db = db, commit = True)
    if is_error: raise RuntimeError("Could not write frame to dummy table, dummy table has been dropped.") 

# Gets the create table statement for a given table with a placeholder for a dummy name and without auto_increment, restrictions against null values or keys
def _get_create_table_query(db, table_name):
    conn, cursor = db.get_conn_objs()
    if not psql.table_exists(table_name,conn,db.flavor):
        _close_all(conn, cursor)
        return
    _close_all(conn, cursor)
    
    create_table_query_raw = execute(query = "show create table " + table_name, db = db, fetch = True)[0][1]
    create_table_query = re.sub(r'CREATE TABLE `[^`]+`','CREATE TABLE `{DUMMY_NAME}`', create_table_query_raw)
    create_table_query = re.sub(r'AUTO_INCREMENT=\d+','', create_table_query)
    create_table_query = re.sub(r'unsigned','', create_table_query)
    create_table_query = re.sub(r'NOT NULL','', create_table_query)
    create_table_query = re.sub(r'AUTO_INCREMENT','', create_table_query)
    create_table_query = re.sub(r'\"(.+?)\"','', create_table_query)
    create_table_query = re.sub(r'PRIMARY KEY(.+?)\),*','', create_table_query)
    create_table_query = re.sub(r'UNIQUE KEY(.+?)\),*','', create_table_query)
    create_table_query = re.sub(r'KEY(.+?)\),*','', create_table_query)
    create_table_query = re.sub(r',\s*\)',')', create_table_query)

    return create_table_query

# Takes basic connection information and forms a basic kwargs dict
def _gen_basic_kwargs(host,port,user,password,db):
    return {'host' : host, 'port' : port, 'user' : user, 'password' : password, 'db' : db}
  
# Formats connection information according to the database flavor
def _gen_formatted_kwargs(basic_kwargs,additional_kwargs,flavor):

    formatted_kwargs = {}

    for field in FLAVOR_INFO['names'].keys():
        if field in basic_kwargs:
            if basic_kwargs[field] is not None:
                formatted_kwargs[FLAVOR_INFO['names'][field][flavor]] = basic_kwargs[field]

    if (type(additional_kwargs) == dict):
        for k, v in additional_kwargs.iteritems():
            if v is not None:
                formatted_kwargs[k] = v

    return formatted_kwargs

# Connects to a database using kwargs
def _connect_with_kwargs(kwargs,flavor,wait,max_tries):
    return _perform_operation(method = partial(_connect, kwargs = kwargs, flavor = flavor), max_tries = max_tries, wait = wait)

# Combines lists of dictionaries with placeholder values
def _gen_whole_format_dict_list(partitions):
    if not isinstance(partitions, (set, tuple, list)): partitions = [partitions]
    individual_format_dict_lists = [partition.format_dict_list for partition in partitions]
    list_of_format_dicts_unjoined = list(itertools.product(*individual_format_dict_lists))
    return [dict((i,j) for individual_format_dict in format_dicts_unjoined for i,j in individual_format_dict.items()) for format_dicts_unjoined in list_of_format_dicts_unjoined]

# Gets query results and maps them
def _expl_map_execute(block):
    result = execute(*block[:-1])
    map_operation = block[-1]
    return _attempt_method(map_operation, result)
    
# Generates a random seed
def _gen_new_random_seed(random_seed = None):
    if random_seed is None: 
        return hashlib.sha224(str(random_seed)).hexdigest()
    return hashlib.sha224(str(time.time())+str(rand())).hexdigest()

# Generates a set of random seeds
def _gen_new_random_seed_set(num, random_seed = None, max_attempts = 10):
    random_seed_set = set()
    while max_attempts > 0:
        random_seed = _gen_new_random_seed(random_seed)
        if random_seed not in random_seed_set:
            random_seed_set.add(random_seed)
        else:
            max_attempts += -1
        if len(random_seed_set) == num: return random_seed_set
    raise RuntimeError("Could not generate unique random seed set.")

# Closes connection objects in proper order
def _close_all(conn, cursor):
    cursor.close()
    conn.close()

# Returns what is passed to it
def _return_given(x=None):
    return x
