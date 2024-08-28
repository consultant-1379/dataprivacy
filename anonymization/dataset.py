import numpy as np
import pandas as pd
import datetime
import yaml
import io
from defs import ColumnType, Column
import defs


class Dataset:

    def __init__(self, path, descriptor, transformation, operation="anonymize"):
        self.operation = operation
        self.path = path
        self.columns_default = Column(name="default", type=ColumnType.STRING, impute="none", impute_value=None, keep_null=False, lower=None, upper=None)
        self.columns = {}
        self.df = None
        self.dtypes = {}

        self.anonymized_columns = []
        self.included_columns = []
        self.raw_columns = []
        self.categorical_encoders = None
        self.descriptor = {}
        self.transformation = {}
        self.columns_with_null = {}

        self.descriptor = defs.load_yaml(descriptor)

        if transformation is not None:
            self.transformation = defs.load_yaml(transformation)

        self.setup_columns()
        self.set_dateformat()
        self.parse_columns_default()
        self.parse_columns()
        self.retrieve_dataset()
        self.sample_dataset()
        self.impute_values()
        self.encode_categoricals()

    def parse_columns_default(self):
        if 'columns_default' in self.descriptor:
            default = self.descriptor['columns_default']
            ctype = default.get('type', self.columns_default.type)
            impute = default.get('impute', self.columns_default.impute)
            impute_value = default.get('impute_value', self.columns_default.impute_value)
            keep_null = default.get('keep_null', self.columns_default.keep_null)
            lower = default.get('lower', self.columns_default.lower)
            upper = default.get('upper', self.columns_default.upper)
            self.columns_default=Column(name="default", type=ctype, impute=impute, impute_value=impute_value, keep_null=keep_null, lower=lower, upper=upper)

    def setup_columns(self):
        if 'columns' not in self.transformation:
            return

        self.anonymized_columns = self.transformation['columns']['anonymize'].split(',')
        self.included_columns = self.transformation['columns']['include'].split(',')

        for c in self.included_columns:
            if c not in self.anonymized_columns:
                self.raw_columns.append(c)

    def get_dataframe(self):
        return self.df

    def get_columns_to_anonymize(self):
        return self.df[self.anonymized_columns]

    def get_anonymized_columns(self):
        return self.anonymized_columns

    def set_dateformat(self):
        if 'dateformat' in self.descriptor:
            defs.dateformat = self.descriptor['dateformat']
        else:
            defs.dateformat = '%m/%d/%Y'

    def retrieve_dataset(self):
        header = self.descriptor.get("header", True)
        if header:
            names = None
        else:
            names = list(self.descriptor['columns'].keys())

        separator = self.descriptor['separator']
        df = pd.read_csv(
            io.FileIO(self.path),
            names=names,
            sep=separator,
            index_col=False,
            converters=self.dtypes)

        self.df = df

    def parse_column(self, cn):
        def to_date(x):
            # month/day/year
            if x is '':
                return None
            return defs.date2timestamp(x)

        def to_int(x):
            if x is '':
                return None
            return int(float(x))

        def to_float(x):
            if x is '':
                return None
            return float(x)

        c = self.descriptor['columns'][cn]
        if c is None:
            c = {}

        t = c.get('type', self.columns_default.type)
        if t == "string":
            dtype = str
            c_type = ColumnType.STRING
        elif t == "float":
            dtype = to_float
            c_type = ColumnType.FLOAT
        elif t == "date":
            dtype = to_date
            c_type = ColumnType.DATE
        elif t == "integer":
            dtype = to_int
            c_type = ColumnType.INTEGER
        elif t == "categorical":
            dtype = str
            c_type = ColumnType.CATEGORICAL
        else:
            raise Exception('Unknown type', t)

        c_impute = c.get("impute", self.columns_default.impute)
        c_impute_value = c.get("impute_value", self.columns_default.impute_value)
        c_keep_null = c.get("keep_null", self.columns_default.keep_null)
        c_lower = c.get("lower", self.columns_default.lower)
        c_upper = c.get("upper", self.columns_default.upper)

        return Column(name=cn, type=c_type, impute=c_impute, impute_value=c_impute_value, keep_null=c_keep_null, lower=c_lower, upper=c_upper), dtype

    def parse_columns(self):
        dtypes = {}
        to_drop = list(self.descriptor['columns'].keys())

        for cn in self.descriptor['columns'].keys():
            to_drop.remove(cn)

            ct, dtype = self.parse_column(cn)

            if self.operation == "anonymize":
                # If column is not to be anonymized, keep it exactly as it was
                if cn in self.raw_columns:
                    dtype = str
                    ct = Column(name=cn, type=ColumnType.STRING, lower=None, upper=None, impute=None, impute_value=None, keep_null=False)

            self.columns[cn] = ct

            dtypes[cn] = dtype

        self.dtypes = dtypes

        # TODO df = df.drop(columns=to_drop)

    def update_columns(self, sub_dataset):
        pass

    def get_decoded_dataset(self, df):
        decoded_dataset = pd.DataFrame(index=df.index.copy())

        for cn in df.columns:
            if self.columns[cn].type is ColumnType.CATEGORICAL:
                categories = len(self.categorical_encoders[cn].classes_)
                decoded_dataset[cn] = df[cn].apply(round) % categories

                decoded_dataset[cn] = self.categorical_encoders[cn].inverse_transform(decoded_dataset[cn])

            if self.columns[cn].type is ColumnType.INTEGER:
                decoded_dataset[cn] = df[cn].apply(round)

            if self.columns[cn].type is ColumnType.FLOAT:
                decoded_dataset[cn] = df[cn]

            if self.columns[cn].type is ColumnType.STRING:
                raise Exception("String columns can not be anonymized!")

            if self.columns[cn].type is ColumnType.DATE:
                decoded_dataset[cn] = df[cn].apply(defs.timestamp2date)

            # Restore all Null values if keep_null is enabled for the column
            if cn in self.columns_with_null:
                ixs = df.index.intersection(self.columns_with_null[cn])
                decoded_dataset.loc[ixs, cn] = None

        # join raw columns
        for rc in self.raw_columns:
            decoded_dataset[rc] = self.df[rc]

        return decoded_dataset

    def encode_categoricals(self):
        from sklearn.preprocessing import LabelEncoder

        self.categorical_encoders = {}

        for cn in self.columns.keys():
            if self.columns[cn].type is ColumnType.CATEGORICAL:
                encoder = LabelEncoder()
                self.df[cn] = encoder.fit_transform(self.df[cn])
                self.categorical_encoders[cn] = encoder

        # DELETE TODO df = df.apply(pd.to_numeric, errors='ignore')
        # data_mem = df.memory_usage(index=True).sum()
        # print("Memory consumed by " + self.path + ": " + str(data_mem))

    def sample_dataset(self):
        if 'row_cap' not in self.transformation:
            return
        row_cap = self.transformation['row_cap']

        if row_cap is not None and len(self.df) > row_cap:
            print("Number of rows  is " + str(self.df.axes[0]) + " > " + str(row_cap) + ", subsampling to " + str(
                row_cap))

            subsampling = self.transformation['subsampling']
            if subsampling == "top":
                self.df = self.df.iloc[0:row_cap]
            elif subsampling == "random":
                self.df = self.df.sample(n=row_cap)
            else:
                print("ERROR: UNKNOWN SUBSAMPLING " + str(subsampling))

    def impute_values(self):
        for cn in self.columns.keys():
            c = self.columns[cn]
            if c.impute is None or c.impute == 'none':
                continue

            if c.keep_null:
                nulls = []
                for index, value in self.df[cn].items():
                    if pd.isna(value):
                        nulls.extend([index])
                self.columns_with_null[cn] = nulls

            if c.impute == 'mean':
                ival = self.df[cn].mean()
            elif c.impute == 'zero':
                ival = 0
            elif c.impute == 'value':
                if c.type == ColumnType.DATE:
                    ival = defs.date2timestamp(c.impute_value)
                else:
                    ival = c.impute_value
            else:
                raise Exception("Unknown imputation type:", c.impute)

            self.df.fillna(axis='index', value=ival, inplace=True)

    def restore_nulls(self, df):
        for cn in self.columns_with_null:
            df.loc[[self.columns_with_null[cn]],[cn]] = None
        return df

