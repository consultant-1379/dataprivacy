import warnings
from anonymizers import anon_lib
import logger

warnings.filterwarnings('ignore')


def anonymize(anonymizer, df):
    epsilon = anonymizer['epsilon']
    cluster_size = anonymizer['cluster_size']

    dataset_size = len(df.index)
    anon_lib.truncate_df(df, cluster_size)
    # df.drop(df.tail(dataset_size % cluster_size).index, inplace=True)

    if dataset_size % cluster_size != 0:
        raise Exception("Cluster size does not match dataset size")
    newDf = anon_lib.get_similar_df(df)

    logger.progress("Processing columns:")
    for col in df.columns:
        logger.progress(col)
        sorted_col = df[col].sort_values()

        for clusterId in range(0, dataset_size // cluster_size):
            cluster = sorted_col.iloc[clusterId * cluster_size:(clusterId + 1) * cluster_size]
            newValue = anon_lib.get_noised_value(cluster, epsilon, cluster_size)
            sorted_col.iloc[clusterId * cluster_size:(clusterId + 1) * cluster_size] = newValue

        newDf[col] = sorted_col.sort_index()

    newDf.drop([1,15,16], inplace=True)
    return newDf
