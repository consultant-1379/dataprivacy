# -*- coding: utf-8 -*-

import argparse
import os
import time

import opendp.smartnoise.core as sn

import logger
from dataset import Dataset
import defs


def setup_parser():
    parser = argparse.ArgumentParser()
    parser.register("type", "bool", lambda v: v.lower() == "true")
    parser.add_argument(
        "--input",
        type=str,
        help="Path the input CSV file.")
    parser.add_argument(
        "--dataset",
        type=str,
        help="Yaml descriptor file of the dataset.")
    parser.add_argument(
        "--queryfile",
        type=str,
        help="Path to the query file to execute.")

    return parser


if __name__ == "__main__":
    flags, _ = setup_parser().parse_known_args()
    for arg in vars(flags):
        print(arg + "='" + str(getattr(flags, arg)) + "'")

    input_path = flags.input
    queryfile = defs.load_yaml(flags.queryfile)

    _dataset = Dataset(flags.input, flags.dataset, None)

    start = int(round(time.time()))

    with sn.Analysis() as analysis:
        # load data
        df = _dataset.get_dataframe()
        queries = queryfile['queries']
        query_names = []
        query_results = []

        for query in queries:
            column_name = query['column']
            epsilon = query['epsilon']
            query_type = query['type']
            n_rows = len(df.index)

            ds = sn.Dataset(value=df[column_name].to_numpy(), value_format='array')

            dt = _dataset.columns[column_name].type
            if dt is defs.ColumnType.FLOAT:
                c_lower = float(_dataset.columns[column_name].lower)
                c_upper = float(_dataset.columns[column_name].upper)
                data = sn.to_float(ds)
            else:
                raise Exception("DP queries only supported for float, not supported for " + str(dt))

            if query_type == "min":
                query_name = "MIN(" + column_name + ")"
                res = sn.dp_minimum(data=data,
                                    privacy_usage={'epsilon': epsilon},
                                    data_lower=c_lower,
                                    data_upper=c_upper,
                                    data_rows=n_rows,
                                    data_columns=1)

            elif query_type == "max":
                query_name = "MAX(" + column_name + ")"
                res = sn.dp_maximum(data=data,
                                    privacy_usage={'epsilon': epsilon},
                                    data_lower=c_lower,
                                    data_upper=c_upper,
                                    data_rows=n_rows,
                                    data_columns=1)

            elif query_type == "mean":
                query_name = defs.get_dp_query_str("MEAN", column_name, c_lower, c_upper)
                res = sn.dp_mean(data=data,
                                 privacy_usage={'epsilon': epsilon},
                                 data_lower=c_lower,
                                 data_upper=c_upper,
                                 data_rows=n_rows,
                                 data_columns=1)

            elif query_type == "sum":
                query_name = defs.get_dp_query_str("SUM", column_name, c_lower, c_upper)
                res = sn.dp_sum(data=data,
                                privacy_usage={'epsilon': epsilon},
                                data_lower=c_lower,
                                data_upper=c_upper,
                                data_rows=n_rows,
                                data_columns=1)

            elif query_type == "variance":
                query_name = defs.get_dp_query_str("VAR", column_name, c_lower, c_upper)
                res = sn.dp_variance(data=data,
                                     privacy_usage={'epsilon': epsilon},
                                     data_lower=c_lower,
                                     data_upper=c_upper,
                                     data_rows=n_rows,
                                     data_columns=1)

            elif query_type == "histogram":
                edges = query['edges']
                query_name = defs.get_dp_query_str("HIST", column_name, c_lower, c_upper) + \
                             " with edges: " + ",".join(map(str, edges))
                res = sn.dp_histogram(data=data,
                                      privacy_usage={'epsilon': epsilon},
                                      edges=edges,
                                      data_lower=c_lower,
                                      data_upper=c_upper,
                                      data_rows=n_rows,
                                      data_columns=1)

            else:
                raise Exception("Invalid query type:", query_type)

            query_names.append(query_name)
            query_results.append(res)

    analysis.release()

    print("Query results:")
    for i in range(len(query_names)):
        print(query_names[i], " ==> ", query_results[i].value)

    logger.log("\nApproximate total privacy loss: epsilon = ", analysis.privacy_usage.approximate.epsilon)

    stop = int(round(time.time()))

    print("\nFinished in ", str(stop - start), " seconds.")
