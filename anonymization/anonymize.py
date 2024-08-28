# -*- coding: utf-8 -*-

import argparse
import os
import time

import yaml

import logger
from anonymizers.common import anonymize
from dataset import Dataset


def store_anonymized_dataset(anonymized_dataset, output_path):
    if output_path is None:
        print("Anonymized dataset:")
        print(anonymized_dataset)
    else:
        anonymized_dataset.to_csv(output_path, index=False)
        logger.log("Anonymized output written to file:", output_path)


def setup_parser():
    parser = argparse.ArgumentParser()
    parser.register("type", "bool", lambda v: v.lower() == "true")
    parser.add_argument(
        "--input",
        type=str,
        help="Path the input CSV file.")
    parser.add_argument(
        "--output",
        type=str,
        help="Path the output CSV file. When unspecified, output will be printed to stdout.")
    parser.add_argument(
        "--anonymizer",
        type=str,
        help="Yaml descriptor file of the anonymizer algorithm to be used along with its parameters.")
    parser.add_argument(
        "--dataset",
        type=str,
        help="Yaml descriptor file of the dataset.")
    parser.add_argument(
        "--dt",
        type=str,
        help="Yaml descriptor file of the dataset transformation.")
    parser.add_argument(
        "--run",
        type=str,
        help="Name of the algorithm configuration to run. Overrides run key in anonymizer file.")

    return parser


if __name__ == "__main__":
    flags, _ = setup_parser().parse_known_args()
    for arg in vars(flags):
        print(arg + "='" + str(getattr(flags, arg)) + "'")

    output_path = flags.output
    if output_path is not None and os.path.exists(output_path):
        print("Error!", output_path, "exists.")
        exit(-1)

    input_path = flags.input

    _dataset = Dataset(flags.input, flags.dataset, flags.dt)

    start = int(round(time.time()))

    anonymizer = None
    if flags.anonymizer is not None:
        with open(flags.anonymizer) as f:
            anonymizer = yaml.load(f, Loader=yaml.FullLoader)

        if flags.run is not None:
            run = flags.run
        else:
            run = anonymizer['run']

        algo = anonymizer['anonymizers'][run]['algorithm']
        anonymized_dataset = None

        anonymized_dataset = anonymize(anonymizer['anonymizers'][run], _dataset)

        store_anonymized_dataset(anonymized_dataset, output_path)
    else:
        print("ERROR: Missing anonymizer yaml!")

    stop = int(round(time.time()))

    print("Finished in ", str(stop - start), " seconds.")
