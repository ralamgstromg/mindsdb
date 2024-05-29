from typing import Optional, Dict
import os
import pandas as pd
from mindsdb.utilities import log
from mindsdb.integrations.libs.base import BaseMLEngine

import polars as pl

# from mlxtend.preprocessing import TransactionEncoder
# from mlxtend.frequent_patterns import fpgrowth
from .transactionencoder import TransactionEncoder
from .fpgrowth import fpgrowth

import pickle

logger = log.getLogger(__name__)


class NgxFpgrowthHandler(BaseMLEngine):
    name = "ngxfpgrowth"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def create(
        self,
        target: str,
        df: Optional[pd.DataFrame] = None,
        args: Optional[Dict] = None,
    ) -> None:
        """Create and train model on given data"""
        # parse args
        if "using" not in args:
            raise Exception(
                "PyCaret engine requires a USING clause! Refer to its documentation for more details."
            )
        using = args["using"]
        if df is None:
            raise Exception("PyCaret engine requires a some data to initialize!")

        using["min_support"] = args.get("min_support", 0.00005)
        using["min_len"] = args.get("min_len", 2)
        using["max_len"] = args.get("max_len", 15)
        using["target_col"] = target

        itemsets = df[target].to_numpy()
        itemsets = [("".join(i).split(";")) for i in itemsets]

        te = TransactionEncoder()
        df_onehot = te.fit_transform(itemsets, as_df=True)

        model_file_path = os.path.join(
            self.model_storage.fileStorage.folder_path, "model"
        )

        model = fpgrowth(
            df_onehot,
            min_support=using["min_support"],
            use_colnames=True,
            max_len=using["max_len"],
        )

        with open(model_file_path, "wb") as f:
            pickle.dump(model, f)

        self.model_storage.json_set(
            "saved_args", {**using, "model_path": model_file_path}
        )

    def predict(
        self, df: Optional[pd.DataFrame] = None, args: Optional[Dict] = None
    ) -> pd.DataFrame:
        """Predict on the given data"""
        saved_args = self.model_storage.json_get("saved_args")
        with open(saved_args["model_path"], "rb") as f:
            model = pickle.load(f)

        to_export = model.with_columns(
            pl.col("itemsets").alias("length").map_elements(lambda x: len(x))
        )
        to_export = to_export.filter(pl.col("length") >= saved_args["min_len"])
        to_export = to_export.with_columns(pl.col("itemsets").map_elements(";".join))
        to_export = to_export.rename({"itemsets": saved_args["target_col"]}, axis=1)

        return to_export.sort("support", ascending=False).to_pandas()

    def describe(self, attribute=None):
        model_args = self.model_storage.json_get("model_args")

        if attribute == "model":
            return pd.DataFrame(
                {k: [model_args[k]] for k in ["model_name", "frequency", "hierarchy"]}
            )

        elif attribute == "features":
            return pd.DataFrame(
                {
                    "ds": [model_args["order_by"]],
                    "y": model_args["target"],
                    "unique_id": [model_args["group_by"]],
                    "exog_vars": [model_args["exog_vars"]],
                }
            )

        elif attribute == "info":
            outputs = model_args["target"]
            inputs = [
                model_args["target"],
                model_args["order_by"],
                model_args["group_by"],
            ] + model_args["exog_vars"]
            accuracies = [
                (model, acc) for model, acc in model_args.get("accuracies", {}).items()
            ]
            return pd.DataFrame(
                {"accuracies": [accuracies], "outputs": outputs, "inputs": [inputs]}
            )

        else:
            tables = ["info", "features", "model"]
            return pd.DataFrame(tables, columns=["tables"])
