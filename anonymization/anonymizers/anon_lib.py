from sklearn.neighbors import LocalOutlierFactor
import pandas as pd
import numpy


def filter_lof(df, k=20):
    lof = LocalOutlierFactor(n_neighbors=k)
    df2 = pd.DataFrame.copy(df)
    df2["_lof"] = lof.fit_predict(df2)
    return df2[df2["_lof"] > 0].drop(columns="_lof")


def get_noised_value(col, epsilon, cluster_size=1):
    base = numpy.mean(col)
    sensitivity = (max(col) - min(col)) / cluster_size
    return numpy.random.laplace(base, sensitivity / epsilon)


def get_scaled_df(df, scaler):
    return pd.DataFrame(scaler.fit_transform(df), columns=df.columns, index=df.index)


def get_unscaled_df(df, scaler):
    return pd.DataFrame(scaler.inverse_transform(df), columns=df.columns, index=df.index)


def get_similar_df(df):
    return pd.DataFrame(index=df.index.copy(), columns=df.columns)


def truncate_df(df, cluster_size):
    df.drop(df.tail(len(df.index) % cluster_size).index, inplace=True)
