import warnings
from anonymizers import anon_lib
import logger

warnings.filterwarnings('ignore')


def anonymize(anonymizer, df):
    epsilon = anonymizer['epsilon']
    cluster_size = anonymizer['cluster_size']

    anon_lib.truncate_df(df, cluster_size)

    dataset_size = len(df.index)

    if dataset_size % cluster_size != 0:
        raise Exception("Cluster size does not match dataset size")
    newDf = anon_lib.get_similar_df(df)
    rankings = anon_lib.get_similar_df(df)

    logger.progress("Sorting columns:")
    for col in df.columns:
        logger.progress(col)
        sorted_col = df[col].sort_values()

        rank = 1
        for i in sorted_col.index:
            rankings.loc[i, col] = rank
            rank = rank + 1
    rankings['_Sum'] = rankings.sum(axis=1)
    rankings['_OriginalIndex'] = rankings.index
    sortedRankings = rankings.sort_values(by=['_Sum'])

    rankIndexes = sortedRankings["_OriginalIndex"]

    for clusterId in range(0, dataset_size // cluster_size):
        if clusterId % 100 == 0:
            logger.progress("Cluster ID:", clusterId)
        # cstart = clusterId
        clusterIndexes = rankIndexes.iloc[clusterId * cluster_size:(clusterId + 1) * cluster_size]
        cluster = df.loc[clusterIndexes]
        newCluster = cluster.apply(anon_lib.get_noised_value, args=[cluster_size, epsilon])
        for i in clusterIndexes:
            newDf.loc[i] = newCluster

    return newDf
