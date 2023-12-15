import io

import numpy as np
import pandas as pd
from sklearn.base import BaseEstimator, TransformerMixin


class BookingDataReadCsv(BaseEstimator, TransformerMixin):
    def __init__(self):
        pass

    def fit(self, X, y=None):
        return self

    def transform(self, X):
        if X is not None:
            return pd.read_csv(
                # X,
                io.StringIO(X),
                header=0,
                names=[
                    "ref_id",
                    "res_id",
                    "hotel_id",
                    "operator_id",
                    "operator_code",
                    "bkg_ref",
                    "guest_name",
                    "sales_date",
                    "in_date",
                    "out_date",
                    "room_type",
                    "room_code",
                    "meal",
                    "days",
                    "adult",
                    "child",
                    "purchase_price",
                    "purchase_currency",
                    "sales_price",
                    "sales_currency",
                    "purchase_price_indicator",
                    "sales_price_indicator",
                    "create_date",
                    "last_modified_date",
                    "cancellation_date",
                    "status",
                    "status4",
                    "status5",
                    "purchase_contract_id",
                    "purchase_spo_id",
                    "sales_contract_id",
                    "sales_spo_id",
                    "sales_spo_name",
                    "sales_spo_code",
                    "purchase_spo_name",
                    "purchase_spo_code",
                    "main_season",
                ],
                dtype={
                    "ref_id": int,
                    "resales_id": int,
                    "hotel_id": int,
                    "operator_id": int,
                    "operator_code": str,
                    "bkg_ref": str,
                    "guest_name": str,
                    "room_type": str,
                    "room_code": str,
                    "meal": str,
                    "days": int,
                    "adult": int,
                    "child": int,
                    "purchase_price": float,
                    "purchase_currency": str,
                    "sales_price": float,
                    "sales_currency": str,
                    "purchase_price_indicator": str,
                    "sales_price_indicator": str,
                    "status": str,
                    "status4": str,
                    "status5": str,
                    "purchase_contract_id": pd.Int64Dtype(),
                    "purchase_spo_id": pd.Int64Dtype(),
                    "sales_contract_id": pd.Int64Dtype(),
                    "sales_spo_id": pd.Int64Dtype(),
                    "sales_spo_name": str,
                    "sales_spo_code": str,
                    "purchase_spo_name": str,
                    "purchase_spo_code": str,
                    "main_season": str,
                },
                parse_dates=[
                    "sales_date",
                    "in_date",
                    "out_date",
                    "create_date",
                    "last_modified_date",
                    "cancellation_date",
                ],
            )