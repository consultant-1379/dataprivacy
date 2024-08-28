import numpy as np
import pandas as pd

# NOTE: Temporary
# We add a memory cap here for now, which
# forces a subsampling of particularly large
# datasets in order to not overwhelm
# joblib 
MEM_CAP = 1500000 # 500KB

def load_data(req_datasets):
    """
    Takes in optional dataset list. Otherwise grabs them
    from the conf.py file.

    Returns a dictionary of datasets.
    {
        'dset': pd.DataFrame,
        'dset': pd.DataFrame
    }
    """
    import requests
    import io
    import json

    with open('datasets.json') as j:
        dsets = j.read()
    archive = json.loads(dsets)

    loaded_datasets = {}

    def retrieve_dataset(dataset):
        if dataset['url'] == "":
            dtypes = {}
            for cn in dataset['columns'].split(','):
                # if "date" in cn:
                #     dtypes[cn] = str
                # else:
                #     dtypes[cn] = float
                # print(str(dtypes))
                dtypes[cn] = lambda x: str(x)

            sep = dataset['sep'] #dataset['name'] +

            skiprows = 0
            if dataset['header'] == "t":
                skiprows = 1

            df = pd.read_csv(io.FileIO(dataset['name'] + ".csv"), names=dataset['columns'].split(','), sep=sep, index_col=False, skiprows=skiprows, converters=dtypes)
            # print("df", str(df))
            # x = df.iloc[0][2]
            # print("dfdf", str(x), type(x))
            # for i in range(df.shape[0]):
            #     x = df.iloc[i][2]
            #     if not isinstance(x, str):
            #         print("FUCK", str(i), x, type(x))
            #         break

            return df

        # r = requests.post(dataset['url'])
        # if r.ok:
        #     data = r.content.decode('utf8')
        #     sep = dataset['sep']
        #     df = pd.read_csv(io.StringIO(data), names=dataset['columns'].split(','), sep=sep, index_col=False)
        #     if dataset['header'] == "t":
        #         df = df.iloc[1:]
        #     return df

        raise "Unable to retrieve dataset: " + dataset

    def select_column(scol):
        # Zero indexed, inclusive
        return scol.split(',')

    def encode_categorical(df,dataset):
        from sklearn.preprocessing import LabelEncoder

        encoders = {}
        if dataset['categorical_columns'] != "":
            for column in select_column(dataset['categorical_columns']):
                encoders[column] = LabelEncoder()
                print("column", str(column))
                df[column] = encoders[column].fit_transform(df[column])

        df = df.apply(pd.to_numeric, errors='ignore')
        data_mem = df.memory_usage(index=True).sum()
        print("Memory consumed by " + dataset['name'] + ":" + str(data_mem))
        if data_mem > MEM_CAP:
            print("Memory use too high with " + dataset['name'] + ", subsampling to:" + str(MEM_CAP))
            reduct_ratio = MEM_CAP / data_mem
            subsample_count = int(len(df.index) * reduct_ratio)
            df = df.iloc[0:subsample_count] #sample(n=subsample_count)
            # print("row", str(df.iloc[0]))
            print("Memory consumed by " + dataset['name'] + ":" + str(df.memory_usage(index=True).sum()))

        return {"data": df, "target": dataset['target'], "name": dataset['name'], "categorical_columns": dataset['categorical_columns']}

    for d in req_datasets:
        df = retrieve_dataset(archive[d])
        encoded_df_dict = encode_categorical(df, archive[d])
        # print("encoded_df_dict", str(encoded_df_dict))
        # a
        loaded_datasets[d] = encoded_df_dict

    # Return dictionary of pd dataframes
    return loaded_datasets
