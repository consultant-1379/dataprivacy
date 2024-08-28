import csv

import numpy as np
from opendp.smartnoise.synthesizers.mwem import MWEMSynthesizer

from load_data import load_data


def get_laplace_noise(cluster, cluster_size, epsilon):
    max_value_in_cluster = np.max(cluster)
    min_value_in_cluster = np.min(cluster)

    sensitivity = (max_value_in_cluster - min_value_in_cluster) / cluster_size

    mean = 0
    stdev = sensitivity / epsilon

    noise = np.random.laplace(mean, stdev)

    return noise

def get_dp_values(values, cluster_size, epsilon):
    s_values = sorted(values)

    indices = range(len(values))
    s_indices = [i for _, i in sorted(zip(values, indices))]

    new_values = [''] * len(values)
    for i in range(0, len(values), cluster_size):
        if all([x is not None for x in s_values[i:i + cluster_size]]):
            cluster = np.array(s_values[i:i + cluster_size]).astype(np.float)
            avg = np.average(cluster)

            for j in range(i, np.min([i + cluster_size, len(values)])):
                noise = get_laplace_noise(cluster, cluster_size, epsilon)
                new_values[s_indices[j]] = avg + noise

    return new_values


def distance(r1, r2):
    s = 0

    for i in range(len(r1)):
        if r1[i] != '' and r2[i] != '':
            s += (float(r1[i]) - float(r2[i])) ** 2
        elif r1[i] != '':
            s += float(r1[i]) ** 2
        elif r2[i] != '':
            s += float(r2[i]) ** 2

    return np.sqrt(s)


def find_nearest_rows(rows, row_idx, columns_to_anonymize):
    row = rows[row_idx]
    projected_row = [row[column_index] for column_index in columns_to_anonymize]
    # projected_row = np.array(projected_row).astype(np.float)

    projected_rows = []
    for r in rows:
        projected_rows.append([r[column_index] for column_index in columns_to_anonymize])
    # projected_rows = np.array(projected_rows).astype(np.float)

    min_distance = np.inf
    for i in range(len(rows)):
        if i != row_idx:
            d = distance(projected_rows[i], projected_row)
            if d < min_distance:
                min_distance = d
                min_distance_idx = i

    print(str(min_distance))
    print(str(projected_row))
    print(str(projected_rows[min_distance_idx]))

    return min_distance_idx


def extract_nonzero(fname):
    """
    extracts nonzero entries from a csv file
    input: fname (str) -- path to csv file
    output: generator<(int, int, float)> -- generator
          producing 3-tuple containing (row-index, column-index, data)
    """
    for (rindex, row) in enumerate(csv.reader(open(fname))):
        if rindex == 0:
            continue
        for (cindex, data) in enumerate(row):
            if data != "":
                try:
                    yield (rindex - 1, cindex, float(data))
                except ValueError:
                    pass


def get_dimensions(fname):
    """
    determines the dimension of a csv file
    input: fname (str) -- path to csv file
    output: (nrows, ncols) -- tuple containing row x col data
    """
    rowgen = (row for row in csv.reader(open(fname)))
    # compute col size
    colsize = len(rowgen.__next__())
    # compute row size
    rowsize = 1 + sum(1 for row in rowgen)
    return (rowsize - 1, colsize)

# >>> X = np.array([[1, 2], [1, 4], [1, 0],
# ...               [10, 2], [10, 4], [10, 0]])
# >>> kmeans = KMeans(n_clusters=2, random_state=0).fit(X)
# >>> kmeans.labels_
# array([1, 1, 1, 0, 0, 0], dtype=int32)
# >>> kmeans.predict([[0, 0], [12, 3]])
# array([1, 0], dtype=int32)
# >>> kmeans.cluster_centers_
# array([[10.,  2.],
#        [ 1.,  2.]])


def create_clusters(rows, row_idx, columns_to_anonymize):
    # row = rows[row_idx]
    # projected_row = [(int(round(float(row[column_index]))) if row[column_index] is not '' else 0)
    #                  for column_index in columns_to_anonymize]
    # projected_row = np.array(projected_row).astype(np.int64)
    #
    # projected_rows = []
    # for r in rows:
    #     projected_rows.append([(int(round(float(r[column_index]))) if r[column_index] is not '' else 0)
    #                            for column_index in columns_to_anonymize])
    # projected_rows = np.array(projected_rows).astype(np.int64)
    #
    # X = coo_matrix(projected_rows).tocsr()
    # kmeans = KMeans(n_clusters=int(len(rows)/cluster_size), random_state=0).fit(X)
    #
    # print(strkmeans.labels_)

    (rdim, cdim) = get_dimensions(input_path)

    # allocate a lil_matrix of size (rdim by cdim)
    # note: lil_matrix is used since we be modifying
    #       the matrix a lot.
    S = lil_matrix((rdim, cdim))

    # add data to S
    for (i, j, d) in extract_nonzero(input_path):
        S[i, j] = d

    # perform clustering
    labeler = KMeans(n_clusters=int(rdim / cluster_size), random_state=0)
    # convert lil to csr format
    # note: Kmeans currently only works with CSR type sparse matrix
    labeler.fit(S.tocsr())

    # print cluster assignments for each row
    for (row, label) in enumerate(labeler.labels_):
        print
        "row %d has label %d" % (row, label)


def sort_column(values):
    s_values = sorted(values)

    indices = range(len(values))
    s_indices = [i for _, i in sorted(zip(values, indices))]

    return s_values, s_indices


def create_lazy_clusters(dataset, columns_to_anonymize, cluster_size, epsilon):  # (rows, columns_to_anonymize):
    rows = dataset.values.tolist()

    # print("ROWS", str(rows))

    # def create_lazy_clusters(rows, columns_to_anonymize):
    s_indices = {}
    s_values = {}

    row_weight = np.zeros(len(rows))
    for column_index, cn in enumerate(columns_to_anonymize):
        print("Column:", column_index, cn)

        s_values[column_index], s_indices[column_index] = sort_column([r[column_index] for r in rows])

        for i in range(len(rows)):
            row_weight[i] += s_indices[column_index][i]

    _, s_row_weight_indices = sort_column(row_weight)

    new_rows = np.copy(rows)
    for index in range(0, len(s_row_weight_indices), cluster_size):
        weight_sorted_rows = [rows[s_row_weight_indices[i]]
                              for i in range(index, np.min([index + cluster_size, len(s_row_weight_indices)]))]

        this_cluster_size = np.min([cluster_size, np.abs(index + cluster_size - len(rows))])
        for column_index, _ in enumerate(columns_to_anonymize):
            column_values = [r[column_index] for r in weight_sorted_rows]

            avg = 0
            min = np.inf
            max = -np.inf
            nonempty_count = 0
            for j in range(this_cluster_size):
                v = column_values[j]

                if type(v) == str:
                    if len(v) > 0:
                        v = float(v)
                        nonempty_count += 1
                        avg += v

                        if min > v:
                            min = v
                        if max < v:
                            max = v
                else:
                    v = float(v)
                    nonempty_count += 1
                    avg += v

                    if min > v:
                        min = v
                    if max < v:
                        max = v

            if nonempty_count > 0:
                avg /= nonempty_count

                noise = get_laplace_noise(min, max, nonempty_count)

                for k in range(this_cluster_size):
                    row_index = s_row_weight_indices[index + k]
                    v = rows[row_index][column_index]
                    if type(v) == str and len(v) > 0:
                        new_rows[row_index][column_index] = avg + noise

    # print(str(new_rows))

    return new_rows


def anonymize_with_mwem(anonymizer, dataset_descriptor, data_transformation):
    # print("anonymizer", str(anonymizer))

    dataset, label_encoders = load_data(dataset_descriptor, input_path, data_transformation)

    columns_to_keep = data_transformation['columns']['include'].split(',')
    columns_to_anonymize = data_transformation['columns']['anonymize'].split(',')
    for cn in columns_to_anonymize:
        columns_to_keep.remove(cn)
    dataset_to_anonymize = dataset.drop(columns=columns_to_keep)

    synth = MWEMSynthesizer(
        Q_count=anonymizer['Q_count'],
        epsilon=anonymizer['epsilon'],
        iterations=anonymizer['iterations'],
        mult_weights_iterations=anonymizer['mult_weights_iterations'],
        split_factor=anonymizer['split_factor'],
        max_bin_count=anonymizer['max_bin_count'])

    synth.fit(dataset_to_anonymize)

    sample_size = len(dataset)
    anonymized = synth.sample(int(sample_size))

    for cn in columns_to_keep:
        anonymized[cn] = dataset[cn]
    for cn in columns_to_anonymize:
        if 'categorical' in dataset_descriptor['columns'][cn].keys():
            if dataset_descriptor['columns'][cn]['categorical']:
                anonymized[cn] = label_encoders[cn].inverse_transform(dataset[cn])

    return anonymized


def anonymize_with_nka(anonymizer, ds):
    dataset, label_encoders = ds.get_dataframe()
    # print(str(dataset))

    epsilon = anonymizer['epsilon']
    cluster_size = anonymizer['cluster_size']
    columns_to_anonymize = data_transformation['columns']['anonymize'].split(',')
    clustering = anonymizer['clustering']

    columns_to_keep = data_transformation['columns']['include'].split(',')
    for cn in columns_to_anonymize:
        columns_to_keep.remove(cn)

    if clustering == "cloumn_wise":
        for cn in columns_to_anonymize:
            print("Column:", cn)
            dataset[cn] = get_dp_values(dataset[cn], cluster_size, epsilon)
    elif clustering == "row_weight":
        dataset = create_lazy_clusters(dataset, columns_to_anonymize, cluster_size, epsilon)

    anonymized = dataset
    for cn in columns_to_keep:
        anonymized[cn] = dataset[cn]
    # for cn in columns_to_anonymize:
    #     if 'categorical' in dataset_descriptor['columns'][cn].keys():
    #         if dataset_descriptor['columns'][cn]['categorical']:
    #             column = dataset[cn].astype(int)
    #             max = column.max()
    #             min = column.min()
    #             for i in range(len(column)):
    #                 if column[i] > max:
    #                     column[i] = max
    #                 if column[i] < min:
    #                     column[i] = min
    #
    #             anonymized[cn] = label_encoders[cn].inverse_transform(column)
    #     elif "date" == dataset_descriptor['columns'][cn]['type']:
    #         column = dataset[cn]
    #         for i in range(len(column)):
    #             timestamp = column[i]
    #             if not math.isnan(timestamp):
    #                 date_time = datetime.datetime.fromtimestamp(timestamp)
    #                 column[i] = date_time.strftime('%m/%d/%Y')

    return anonymized

