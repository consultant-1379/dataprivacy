from sklearn.preprocessing import StandardScaler
import sklearn.neighbors

import warnings
from anonymizers import anon_lib
import logger

warnings.filterwarnings('ignore')


def get_k_sphere(df, k, index, tree):
    # indexes = tree.query(df[index:index + 1], k + 1, return_distance=False)
    indexes = tree.query(df.iloc[[index]], k + 1, return_distance=False)
    return df.iloc[indexes[0]]


def process_line(df, k, epsilon, index, tree):
    sphere = get_k_sphere(df, k, index, tree)
    return sphere.apply(anon_lib.get_noised_value, args=[epsilon])


def anonymize(anonymizer, df):
    epsilon = anonymizer['epsilon']
    k  = anonymizer['k']
    lof = anonymizer['lof']

    scaler = StandardScaler()
    scaledDf = anon_lib.get_scaled_df(df, scaler)

    if lof:
        scaledDf = anon_lib.filter_lof(scaledDf)

    anon_df = anon_lib.get_similar_df(df)

    tree = sklearn.neighbors.BallTree(scaledDf, metric="euclidean")

    for i in range(0, len(scaledDf)):
        anon_df.iloc[i] = process_line(scaledDf, k, epsilon, i, tree)
        if i % 50 == 0:
            logger.log("Processing", i)
    return anon_lib.get_unscaled_df(anon_df, scaler)
