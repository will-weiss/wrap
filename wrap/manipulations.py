# These are a set of methods for converting iterable objects to SQL statements.

import datetime
import time
import pandas as pd
import copy
import itertools
import math

def column_to_check_inclusion(column, literal = True, num = 1):
    """
    Returns parenthetical string(s) corresponding to a SQL statement checking for inclusion in the values of the column.

    column : iterable (set, tuple, list, pandas.core.series.Series, pandas.core.frame.DataFrame)
        values to check for inclusion
        if column is a pandas.core.frame.DataFrame, the first column is used
    literal : boolean, default True
        If true the values of column are surrounded by apostrophes
    num : int, default 1
        The number of strings to return
        if num > 1 the strings are returned in a list
    """
    
    if type(column) == pd.core.frame.DataFrame:
        column = pd.Series(column.ix[:,0])

    if len(column) == 0:
        return

    check_str_list = []

    num_remaining = min(len(column), num)

    while num_remaining > 0:
        chunksize = len(column) / num_remaining
        chunk = column[:chunksize]
        column = column[chunksize:]
        
        check_str = "("
        for i in chunk:
            if literal:
                check_str += "'" + str(i) + "',"
            else:
                check_str += str(i) + ","
        check_str = check_str[:-1] + ")"
        check_str_list.append(check_str)
        
        num_remaining += -1

    if num == 1:
        return check_str_list[0]
    else:
        return check_str_list

class label(object):
    """
    label objects are passed to manipulations.df_to_check_where and indicate how columns are to be handled.

    df_label : string
        the name of a column label in a dataframe
    db_column_name : string
        how the column name appears in a SQL query
    comparison : string, default "="
        the comparison to be made
    literal : boolean, default True
        If true the values of column are surrounded by apostrophes
    """
    def __init__(self, df_label, db_column_name, comparison = "=", literal = True):
        self.df_label = df_label
        self.db_column_name = db_column_name
        self.comparison = comparison
        self.literal = literal

def df_to_check_where(dataframe, labels, num = 1):
    """
    Returns nested parenthetical string(s) corresponding to a SQL where statement.

    dataframe : pd.core.frame.DataFrame
    labels : manipulations.label or list of manipulations.label objects
    num : int, default 1
        The number of strings to return
        if num > 1 the strings are returned in a list
    """
    
    if type(dataframe) != pd.core.frame.DataFrame:
        raise RuntimeError("First argument must be a pandas dataframe object.")

    if labels is None:
        raise RuntimeError("At least one manipulations.label object must be passed to manipulations.df_to_check_where.")
    elif not isinstance(labels, (set, list, tuple)):
        labels = [lables]

    if dataframe.shape[0] == 0:
        return

    check_str_list = []

    num_remaining = min(dataframe.shape[0], num)

    while num_remaining > 0:
        chunksize = dataframe.shape[0] / num_remaining
        chunk = dataframe.iloc[:chunksize]
        if num_remaining > 1:
            dataframe = dataframe.iloc[chunksize:]
        
        check_str = "("
        for _, vals in chunk.iterrows():
            check_str += "("
            for label in labels:
                label_val = vals[label.df_label]
                db_column_name = label.db_column_name
                comparison = label.comparison
                literal = label.literal
                check_str += db_column_name + " " + comparison + " "
                if literal:
                    check_str += "'" + str(label_val) + "'"
                else:
                    check_str += str(label_val)
                check_str += " and "
            check_str = check_str[:-5] + ") or "
        check_str = check_str[:-4] + ")"

        check_str_list.append(check_str)
        
        num_remaining += -1
        
    if num == 1:
        return check_str_list[0]
    else:
        return check_str_list