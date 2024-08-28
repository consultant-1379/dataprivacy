import os
import pandas as pd
import numpy as np
from sklearn.linear_model import LogisticRegression
from sklearn.naive_bayes import ComplementNB
from sklearn.linear_model import RidgeClassifier

import utils

from opendp.smartnoise.synthesizers.mwem import MWEMSynthesizer

from load_data import load_data

dsname = "adult"
# dsname = "churn"

# datasets = load_data(['adult'])
datasets = load_data([dsname])


# adult = datasets['adult']['data']
# adult_cat_ord = datasets['adult']['data'].copy()

adult = datasets[dsname]['data']
# adult_cat_ord = datasets[dsname]['data'].copy()
#
print(str(adult))
#
# cat_ord_columns = ['workclass',
#                        'marital-status',
#                        'occupation',
#                        'relationship',
#                        'race',
#                        'gender',
#                        'native-country',
#                        'income',
#                        'education',
#                        'age',
#                        'education-num',
#                        'hours-per-week',
#                   'earning-class']
#
# for c in adult_cat_ord.columns.values:
#     if not c in cat_ord_columns:
#         adult_cat_ord = adult_cat_ord.drop([c], axis=1)

# synth = MWEMSynthesizer(400, 3.00, 40, 20, split_factor=7, max_bin_count=400)

# synth = MWEMSynthesizer(500, 0.1, 30, 15, splits=[[0,1,2],[3,4,5],[6,7,8],[9,10],[11,12],[13,14]], max_bin_count=400)
# synth.fit(datasets[dsname]['data'])

synth_cat_ord = MWEMSynthesizer(500, 1, 30, 15, split_factor=3)
# synth_cat_ord.fit(adult_cat_ord)
synth_cat_ord.fit(adult)

sample_size = len(adult)
# synthetic = synth.sample(int(sample_size))
synthetic_cat_ord = synth_cat_ord.sample(int(sample_size))

# utils.test_real_vs_synthetic_data(adult, synthetic, RidgeClassifier, tsne=True, box=True, describe=True)
utils.test_real_vs_synthetic_data(adult, synthetic_cat_ord, RidgeClassifier, tsne=True, box=True, describe=True)

