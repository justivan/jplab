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
                    "ref_id": pd.Int64Dtype(),
                    "res_id": pd.Int64Dtype(),
                    "hotel_id": pd.Int64Dtype(),
                    "operator_id": pd.Int64Dtype(),
                    "operator_code": str,
                    "bkg_ref": str,
                    "guest_name": str,
                    "room_type": str,
                    "room_code": str,
                    "meal": str,
                    "days": pd.Int64Dtype(),
                    "adult": pd.Int64Dtype(),
                    "child": pd.Int64Dtype(),
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


class BookingDataEncoder(BaseEstimator, TransformerMixin):
    def __init__(self):
        self.exclude = [
            1,  # NO ACCOMMODATION
            2,  # FLIGHT ONLY PAX
            3,  # TOUR ONLY PAX
            4,  # Transfer Only Pax
            5,  # SPLIT BOOKINGS
            5000,  # TEST
            5005,  # TEST HOTEL
            90001,  # HOTEL SHOP RESERVATIONS
            191680,  # ROULETTE OFFER
            202356,  # TEST HOTEL
            203441,  # ROULETTE HOTEL RAK
            209384,  # TEST HOTEL - BUGFIX - DO NOT ACCESS
            100,  # HOTEL SHOP RESERVATIONS
            90018,  # HOTEL SHOP RESERVATIONS
            209385,  # TEST HOTEL - BUGFIX - DO NOT ACCESS
            217636,  # TEST HOTEL
            218648,  # CRUISE
            218736,  # TEST HOTEL
            219137,  # PP_NOACCOM
            219138,  # PP_NOACCOM
        ]

    def fit(self, X, y=None):
        return self

    def transform(self, X):
        if X is not None:
            # Operator Code
            X["operator_code"] = X["operator_code"].fillna(X["hotel_id"]).apply(str)

            # Text Case
            for col in ["guest_name", "room_code", "meal"]:
                X[col] = X[col].str.upper()

            for col in ["status", "status4", "status5"]:
                X[col] = X[col].str.capitalize()

            # Drop non-accommodation entries
            X.drop(X[X["hotel_id"].isin(self.exclude)].index, inplace=True)

            # Cancellation Date
            # Convert "1900-01-01" to pd.NA
            X["cancellation_date"] = X["cancellation_date"].replace("1900-01-01", pd.NA)

            # Fill missing values with "last_modified_date" where "status" is "Can"
            X.loc[
                X["cancellation_date"].isna() & (X["status"] == "Can"),
                "cancellation_date",
            ] = X["last_modified_date"]

            # Set non-matching "status" values to pd.NA
            X.loc[
                ~X["cancellation_date"].isna() & (X["status"] != "Can"),
                "cancellation_date",
            ] = pd.NA

            # Price Info
            X.loc[X["purchase_price"] < 1, "purchase_price"] = 0
            X.loc[X["sales_price"] < 1, "sales_price"] = 0

            # Fill the rest of blank fields with 0 if numerical
            X = X.apply(lambda x: x.fillna(0) if x.dtype.kind in "biufc" else x)

            # Room Code
            X["room_code"] = X["room_code"].apply(lambda x: "" if len(str(x)) > 3 else x)

            # Meal Code
            X["meal"] = X["meal"].apply(lambda x: "UD" if len(str(x)) > 3 else x)
            X["meal"] = X["meal"].str.replace(r"(?i)\*| NO", "", regex=True).str.strip()
            X["meal"] = X["meal"].replace(["", "0", None], "UD")
            X["meal"] = X["meal"].replace(["AIC", "AIA"], "AIU")

            # Fill the rest of blank fields with None for database insertion
            X.replace({"": None, pd.NaT: None, pd.NA: None, np.NaN: None}, inplace=True)

            # Drop fields
            X.drop(["main_season"], axis=1, inplace=True)

            # Drop if data is empty
            X.drop(X[X["ref_id"] == 0].index, inplace=True)

            # Drop duplicates
            X.drop_duplicates(subset="ref_id", keep="last", inplace=True)

            return X
        return None


class HotelDataReadCsv(BaseEstimator, TransformerMixin):
    def __init__(self):
        pass

    def fit(self, X, y=None):
        return self

    def transform(self, X):
        if X is not None:
            return pd.read_csv(
                io.StringIO(X),
                usecols=[
                    "HotelID",
                    "HotelName",
                    "Country",
                    "State",
                    "City",
                    "HotelTypeID",
                    "Longitude",
                    "Latitude",
                    "Giata",
                    "SaleMail",
                    "CreateDate",
                    "LastChangeDate",
                    "IsActive",
                ],
                dtype={
                    "HotelID": int,
                    "HotelName": str,
                    "Country": str,
                    "State": str,
                    "City": str,
                    "HotelTypeID": str,
                    "Longitude": float,
                    "Latitude": float,
                    "Giata": pd.Int64Dtype(),
                    "SaleMail": str,
                    "IsActive": str,
                },
                parse_dates=[
                    "CreateDate",
                    "LastChangeDate",
                ],
                date_format="%d-%b-%y",
            )


class HotelDataEncoder(BaseEstimator, TransformerMixin):
    def __init__(self):
        self.exclude = [
            1,  # NO ACCOMMODATION
            2,  # FLIGHT ONLY PAX
            3,  # TOUR ONLY PAX
            4,  # Transfer Only Pax
            5,  # SPLIT BOOKINGS
            5000,  # TEST
            5005,  # TEST HOTEL
            90001,  # HOTEL SHOP RESERVATIONS
            191680,  # ROULETTE OFFER
            202356,  # TEST HOTEL
            203441,  # ROULETTE HOTEL RAK
            209384,  # TEST HOTEL - BUGFIX - DO NOT ACCESS
            100,  # HOTEL SHOP RESERVATIONS
            90018,  # HOTEL SHOP RESERVATIONS
            209385,  # TEST HOTEL - BUGFIX - DO NOT ACCESS
            217636,  # TEST HOTEL
            218648,  # CRUISE
            218736,  # TEST HOTEL
            219137,  # PP_NOACCOM
            219138,  # PP_NOACCOM
        ]

    def fit(self, X, y=None):
        return self

    def transform(self, X):
        if X is not None:
            X.drop(X[X["HotelID"].isin(self.exclude)].index, inplace=True)
            X.rename(columns={"HotelName": "HotelName_GWG"}, inplace=True)
            X["HotelName_GWG"] = X["HotelName_GWG"].str.split().str.join(" ")
            X["HotelName"] = X["HotelName_GWG"].str.split("(").str[0]
            X["HotelName"] = X["HotelName"].str.replace("-", " ", regex=False)
            X["HotelName"] = X["HotelName"].str.replace("+", " ", regex=False)
            X["HotelName"] = X["HotelName"].str.replace(" AND ", " & ", regex=False)
            X["HotelName"] = X["HotelName"].str.split().str.join(" ")
            X["SaleMail"] = X["SaleMail"].fillna("undefined").str.lower()
            X.sort_values(["Country", "HotelName"], inplace=True)

            return X
        return None
