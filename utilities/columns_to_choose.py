import pandas as pd
import numpy as np
from sqlalchemy.orm import Session
from utilities.group import group_by_bee,group_by_dic_prathisham,group_by_dic_zing, group_by_dic_adb


def get_column_names(db: Session, models, business: str):
    if business == "beelittle":
        groupby = group_by_bee
    elif business == "prathiksham":
        groupby = group_by_dic_prathisham
    elif business == "zing":
        groupby = group_by_dic_zing
    elif business == "adoreaboo":
        groupby = group_by_dic_adb
    else:
        print("Business name is wrong")

    standard_aggregations = {"sum", "mean"}
    dimensions = [col for col, agg in groupby.items() if not isinstance(agg, str) or agg not in standard_aggregations]
    aggregations = [col for col, agg in groupby.items() if isinstance(agg, str) and agg in standard_aggregations]

    # Make both lists equal in length by padding with empty strings
    max_length = max(len(dimensions), len(aggregations))
    dimensions += [""] * (max_length - len(dimensions))
    aggregations += [""] * (max_length - len(aggregations))

    # Create DataFrame
    df = pd.DataFrame({"Dimension": dimensions, "Aggregation": aggregations})


    return df