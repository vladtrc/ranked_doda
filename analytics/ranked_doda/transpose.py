from itertools import chain
from pyspark.sql import DataFrame

def _sort_transpose_tuple(tup):
    x, y = tup
    return x, tuple(zip(*sorted(y, key=lambda v_k: v_k[1], reverse=False)))[0]


def transpose(X):
    cols = X.columns
    n_features = len(cols)

    # Sorry for this unreadability...
    return X.rdd.flatMap( # make into an RDD
        lambda xs: chain(xs)).zipWithIndex().groupBy( # zip index
        lambda val_idx: val_idx[1] % n_features).sortBy( # group by index % n_features as key
        lambda grp_res: grp_res[0]).map( # sort by index % n_features key
        lambda grp_res: _sort_transpose_tuple(grp_res)).map( # maintain order
        lambda key_col: key_col[1]).toDF() # return to DF

