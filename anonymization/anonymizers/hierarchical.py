#TODO replace loc with ILOC!!!

from sklearn.preprocessing import StandardScaler

import warnings
from scipy.cluster import hierarchy

from anonymizers import anon_lib
from logger import log


warnings.filterwarnings('ignore')


def get_clusters(k, tree, clusters) :
    if tree.is_leaf():
        raise Exception("Cluster contains only 1 element")
    dropx = 50
    left = tree.get_left()
    right = tree.get_right()
    leftSize = left.get_count()
    rightSize = right.get_count()
    # print(leftSize, rightSize)
    if (leftSize >= k) and (rightSize >= k):
        get_clusters(k, left, clusters)
        get_clusters(k, right, clusters)
    elif (leftSize >= k) and (rightSize * dropx <= leftSize):
        print("Dropped elements:", rightSize)
        print(right.pre_order())
        get_clusters(k, left, clusters)
    elif (rightSize >= k) and (leftSize * dropx <= rightSize):
        print("Dropped elements:", leftSize)
        print(left.pre_order())
        get_clusters(k, right, clusters)
    else:
        clusters.append(tree.pre_order())


def anonymize(anonymizer, df):
    epsilon = anonymizer['epsilon']
    k  = anonymizer['k']
    lof = anonymizer['lof']

    scaler = StandardScaler()
    scaledDf = anon_lib.get_scaled_df(df, scaler)
    if lof:
        scaledDf = anon_lib.filter_lof(scaledDf)

    anon_df = anon_lib.get_similar_df(df)

    linked = hierarchy.linkage(scaledDf, 'ward')
    tree = hierarchy.to_tree(linked)
    clusters = []
    get_clusters(k, tree, clusters)
    maxClusterSize = 0
    # print(clusters)
    for clusterIndexes in clusters:
        if maxClusterSize < len(clusterIndexes):
            maxClusterSize = len(clusterIndexes)
        cluster = scaledDf.loc[clusterIndexes]
        newCluster = cluster.apply(anon_lib.get_noised_value, args=[epsilon])
        for i in clusterIndexes:
            anon_df.loc[i] = newCluster

    log("Max cluster size:", maxClusterSize)
    return anon_lib.get_unscaled_df(anon_df, scaler)