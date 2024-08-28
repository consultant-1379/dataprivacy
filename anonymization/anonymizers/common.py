import anonymizers.ldiverstiy_columnwise
import anonymizers.ldiverstiy_ordered
import anonymizers.ksphere
import anonymizers.hierarchical

import logger


def anonymize(anonymizer, ds):
    logger.log("Anonymizer parameters:", str(anonymizer))
    dataset = ds.get_columns_to_anonymize()

    algo = anonymizer['algorithm']

    if algo == "ksphere":
        anonymized_df = anonymizers.ksphere.anonymize(anonymizer, dataset)

    elif algo == "hierarchical":
        anonymized_df  = anonymizers.hierarchical.anonymize(anonymizer, dataset)

    elif algo == "ldiverstiy_columnwise":
        anonymized_df  = anonymizers.ldiverstiy_columnwise.anonymize(anonymizer, dataset)

    elif algo == "ldiverstiy_ordered":
        anonymized_df  = anonymizers.ldiverstiy_ordered.anonymize(anonymizer, dataset)

    else:
        raise Exception("ERROR: Unknown algorithm! ", algo)

    return ds.get_decoded_dataset(anonymized_df)
