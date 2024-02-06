import io
from datetime import date
from ftplib import FTP

import pandas as pd
import numpy as np
from config import Config


class OperatorCost:
    def __init__(self):
        self.ftp_server = Config.FTP_SERVER
        self.ftp_username = Config.FTP_USERNAME
        self.ftp_password = Config.FTP_PASSWORD
        self.current_date = date.today().strftime("%Y_%m_%d")

    def get(self):
        ftp = FTP(self.ftp_server)
        ftp.login(self.ftp_username, self.ftp_password)
        ftp.cwd("group_report/")

        csv_files = [
            x
            for x in ftp.nlst()
            if ("UAE" in x or "OM" in x) and self.current_date in x
        ]

        if csv_files:
            df_set = []
            for csv in csv_files:
                cols = [0, 1, 3] if csv.startswith("YT") else [0, 1, 5]
                data = io.BytesIO()
                data.seek(0)
                ftp.retrbinary("RETR " + csv, data.write)
                data.seek(0)
                df = pd.read_csv(
                    data,
                    sep=";",
                    usecols=cols,
                    header=None,
                    names=["bkg_ref", "operator_code", "operator_price"],
                    thousands=",",
                    dtype={
                        "bkg_ref": str,
                        "operator_code": str,
                        "operator_price": float,
                    },
                )
                df = (
                    df.groupby(["bkg_ref", "operator_code"])["operator_price"]
                    .sum()
                    .reset_index(name="operator_price")
                )
                df.dropna(inplace=True)
                df_set.append(df)

            return pd.concat(df_set, axis=0, ignore_index=True)
        return None

    def set(self, df):
        operator_cost = self.get()
        
        if operator_cost is not None:
            df = df.merge(
                operator_cost,
                how="left",
                on=["bkg_ref", "operator_code"],
            )

            df["operator_price"] = (
                df["operator_price"].div(df["count"]).round(2).fillna(0)
            )

            df["purchase_id"] = np.where(
                df["purchase_spo_id"] == 0,
                df["purchase_contract_id"],
                df["purchase_spo_id"],
            ).astype(int)

            df["sales_id"] = np.where(
                df["sales_spo_id"] == 0,
                df["sales_contract_id"],
                df["sales_spo_id"],
            ).astype(int)

            df.drop(
                [
                    "count",
                    "operator_code",
                    "purchase_contract_id",
                    "purchase_spo_id",
                    "sales_contract_id",
                    "sales_spo_id",
                ],
                axis=1,
                inplace=True,
            )

            df.insert(18, "operator_price", df.pop("operator_price"))
            df.insert(19, "purchase_id", df.pop("purchase_id"))
            df.insert(21, "sales_id", df.pop("sales_id"))
            df.insert(1, "analyst", "")
            df.insert(20, "diff", "")
            df.insert(21, "margin", "")

            return df
        return None