# Anonymizer

## Usage

```
usage: anonymize.py [-h] [--input INPUT] [--output OUTPUT]
                    [--anonymizer ANONYMIZER] [--dataset DATASET] [--dt DT]
                    [--run RUN]

optional arguments:
  -h, --help            show this help message and exit
  --input INPUT         Path the input CSV file.
  --output OUTPUT       Path the output CSV file. When unspecified, output
                        will be printed to stdout.
  --anonymizer ANONYMIZER
                        Yaml descriptor file of the anonymizer algorithm to be
                        used along with its parameters.
  --dataset DATASET     Yaml descriptor file of the dataset.
  --dt DT               Yaml descriptor file of the dataset transformation.
  --run RUN             Name of the algorithm configuration to run. Overrides
                        run key in anonymizer file.                  
```

The input CVS must start with a header.

## Example run
```
python anonymize.py  --input examples/test/test20.csv --anonymizer examples/algo.yml --run config3 --dataset examples/test/test_ds.yml --dt examples/test/test_dt.yml
```
```
python anonymize.py --input examples/iot/iot1k.csv --output examples/iot/iot1k_anon.csv  --anonymizer examples/algo.yml --run config1 --dataset examples/iot/iot_ds.yml --dt examples/iot/iot_dt.yml
```
# Differentially private query tool
## Usage
```
usage: dp_query.py [-h] [--input INPUT] [--dataset DATASET]
                   [--queryfile QUERYFILE]

optional arguments:
  -h, --help            show this help message and exit
  --input INPUT         Path the input CSV file.
  --dataset DATASET     Yaml descriptor file of the dataset.
  --queryfile QUERYFILE
                        Path to the query file to execute.
```
## Example run
```
python dp_query.py --input examples/iot/iot1k.csv --dataset examples/iot/iot_ds.yml --queryfile examples/iot/iot_queries.yml
```
# Statistical reporting tool
## Usage

```
usage: statistics.py [-h] [--targetdir TARGETDIR] [--raw RAW] [--anon ANON]
                     [--dataset DATASET] [--dt DT]

optional arguments:
  -h, --help            show this help message and exit
  --targetdir TARGETDIR
                        Report target directory.
  --raw RAW             Path the original CSV file.
  --anon ANON           Path the anonymized CSV file.
  --dataset DATASET     Yaml descriptor file of the dataset.
  --dt DT               Yaml descriptor file of the dataset transformation.
```

## Example run
```
python statistics.py  --raw examples/iot/iot1k.csv  --anon examples/iot/iot1k_anon.csv --targetdir myreport --dataset examples/iot/iot_ds.yml --dt examples/iot/iot_dt.yml
```
# Appendix
## Configuration files
### Dataset file
```
type: csv
separator: ','
header: True
dateformat: '%m/%d/%Y'

# Default settings for all columns
# Values set for a specific column (under "columns" ) will override these values
columns_default:
  # data type: integer|float|date|categorical|string
  type: float
  # imputation for missing values: none|zero|mean|value
  impute: none

  # impute this specific value
  # only used when "impute: value" is set
  # impute_value: 99

  # lower bound (required for DP queries only)
  lower: 0

  # upper bound (required for DP queries only)
  upper: 20

  # Restore all cells with missing data in the specified column after anonymization as missing values (even if there were imputed before the anonymization)
  # WARNING: setting this to True for any column may reduce the level of privacy provided!
  keep_null: False

# Unique settings for individual columns
columns:
  c1:
    type: float
    impute: zero
    lower: 0.0
    # upper: 20.0
  c2:
    type: integer
  c3:
    type: float
    upper: 100
    keep_null: True
  c4:
    type: float
  c5:
    type: categorical

  c6:
    type: date
    impute: value
    impute_value: 1/1/2048
    keep_null: True
```

### Dataset transformation file
```
columns:
  include: c1,c2,c3,c4,c5,c6
  anonymize: c2,c3,c4,c5,c6

```

### Anonymizer configuration file
```
# Anonymizer config name to run by default
# Can be overridden with the --run command line option
run: config1

# Anonymizer configurations
# Contains selected algorithm name and related parameters
# Algorithm names can be: ldiverstiy_columnwise|ldiverstiy_ordered|ksphere|hierarchical
anonymizers:
  config1:
    algorithm: ldiverstiy_columnwise
    cluster_size: 5
    epsilon: 4

  config2:
    algorithm: ldiverstiy_ordered
    cluster_size: 4
    epsilon: 1

  config3:
    algorithm: ksphere
    k: 5
    epsilon: 5
    lof: false

  config4:
    algorithm: hierarchical
    k: 3
    epsilon: 1
    lof: false
```

### Differentially private query file
```
queries:
    # Which column to query
  - column: c1
    # Query type can be: mean|sum|variance|histogram
    type: mean
    # Privacy budget
    epsilon: 9.1
  - column: c3
    type: sum
    epsilon: 99.1
  - column: c1
    type: variance
    epsilon: 99.1
  - column: c1
    type: histogram
    epsilon: 2.9
    # Bin edges fot the histogram
    edges: [1.,4.,11.]
```

## Algorithms supported

All algorithms group certain values, calculate their average, 
and add laplace noise calculated as follows:

```
sensitivity = (max_value_in_cluster-min_value_in_cluster) / cluster_size

mean = 0
stdev = sensitivity / epsilon

noise = np.random.laplace(mean, stdev)
```

### Column-wise noisy L-diversity (Algorithm 1)

This is the default algorithm.

Based on this presentation (see Slide 12):
https://ripe74.ripe.net/presentations/20-Ripe-Presentation.pdf

For each column we cluster the values, and for each cluster we calculate the average and add Laplace noise. 

### Ordered column weighted noisy L-diversity (Algorithm 2)

First the rows are clustered as follows:
1. Each column that has to be anonymized is sorted; 
we get the position of each row wrt each sorted column.
2. The weight of a row is the sum of their positions.
3. We sort the rows based on their weights.
4. The clusters are formed with subsequent rows.

For each column in each cluster of rows, we calculate the 
average of elements and Laplace noise, and we assign their 
sum to the corresponding items.

### Noisy mean with hierarchical clustering (Algorithm 3)
### Noisy mean with KNN (Algorithm 4)

