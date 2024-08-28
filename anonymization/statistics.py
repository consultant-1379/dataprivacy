# -*- coding: utf-8 -*-

import argparse
import os

import matplotlib.pyplot as plt

from dataset import *

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.register("type", "bool", lambda v: v.lower() == "true")
    parser.add_argument(
        "--targetdir",
        type=str,
        help="Report target directory.")
    parser.add_argument(
        "--raw",
        type=str,
        help="Path the original CSV file.")
    parser.add_argument(
        "--anon",
        type=str,
        help="Path the anonymized CSV file.")
    parser.add_argument(
        "--dataset",
        type=str,
        help="Yaml descriptor file of the dataset.")
    parser.add_argument(
        "--dt",
        type=str,
        help="Yaml descriptor file of the dataset transformation.")

    flags, args = parser.parse_known_args()
    for arg in vars(flags):
        print(arg + "='" + str(getattr(flags, arg)) + "'")

    # dataset = None
    # with open(flags.dataset) as f:
    #     dataset = yaml.load(f, Loader=yaml.FullLoader)
    #
    # data_transformation = None
    # with open(flags.dt) as f:
    #     data_transformation = yaml.load(f, Loader=yaml.FullLoader)
    #
    # df = load_data(flags.input, dataset, data_transformation)

    target_dir = flags.targetdir
    _dataset = Dataset(flags.raw, flags.dataset, flags.dt)
    anon_dataset = Dataset(flags.anon, flags.dataset, flags.dt)

    df = _dataset.get_dataframe()
    print("a", df.describe(include='all').transpose().columns[0])

    anon_df = anon_dataset.get_dataframe()

    # print(str(df))
    # print(str(df.count()))
    # print(str(df.sum()))
    # print(str(df.mean()))
    # print(str(df.median()))
    # print(str(df.std()))
    # print(str(df.min()))
    # print(str(df.max()))

    basic_stats = pd.concat([df.describe(include='all').transpose(), anon_df.describe(include='all').transpose()],
                            axis=1)
    print(basic_stats)

    # df = pd.concat([df, anon_df], axis=1)

    os.mkdir(target_dir)
    html_file_path = os.path.join(target_dir, "index.html")
    html_file = open(html_file_path, "w", encoding='utf-8')

    html_file.write("<html><head><title>Anonymization report</title></head><body>\n")

    html_file.write("<h1>Basic stats</h1>\n")
    html_file.write(basic_stats.to_html())

    number_of_columns = len(_dataset.get_anonymized_columns())

    html_file.write("<h1>Columns</h1>\n")

    plt.figure()
    row = 0
    for c in _dataset.get_anonymized_columns():
        print(c)

        html_file.write("<h2>" + c + "</h2>\n")

        dfc = df[[c]]
        anon_dfc = anon_df[[c]]

        raw_value_set = set(np.concatenate(dfc.values.tolist()))
        anon_value_set = set(np.concatenate(anon_dfc.values.tolist()))

        # print(len(raw_value_set))
        # print(len(anon_value_set))

        html_file.write("<p>Number of raw values:" + str(len(raw_value_set)) + "</p>\n")
        html_file.write("<p>Number of anonymzed values:" + str(len(anon_value_set)) + "</p>\n")

        counter = 0
        for r in raw_value_set:
            if r in anon_value_set:
                counter += 1
        html_file.write("<p>Number of raw values in the anonymized value set:" + str(counter) + "</p>\n")
        # print(str(counter))

        _df = pd.concat([dfc, anon_dfc], axis=1)
        _df.columns = [c + " raw", c + " anonymized"]
        print(_df)

        plt.clf()
        # fig, axes = plt.subplots(nrows=1, ncols=2, figsize=(8, 4))
        try:
            ax = _df.plot.kde()  # bins=12, alpha=0.5
        except:
            pass
        plt.savefig(os.path.join(target_dir, c + '_kde.png'))
        html_file.write("<img src=\"" + (c + '_kde.png') + "\" alt=\"kde\">\n")

        plt.clf()
        ax = _df.plot.box()
        plt.savefig(os.path.join(target_dir, c + '_plot.png'))
        html_file.write("<img src=\"" + (c + '_plot.png') + "\" alt=\"plot\">\n")

        html_file.flush()

        row += 1

    html_file.write("</body></html>\n")
