import pandas as pd
from config import Config
from sqlalchemy import MetaData, Table, and_, create_engine, or_, select
from sqlalchemy.orm import Session


class Database:
    TABLES = [
        "mapping_hotel",
        "mapping_hotel_room",
        "mapping_operator",
        "clients_operator",
        "accommodation_hotel_room",
        "accommodation_hotel",
        "accommodation_purchase_manager",
        "definitions_meal_plan",
        "definitions_area",
        "definitions_region",
        "gwg_reservation",
        "users_user",
    ]

    def __init__(self):
        self.engine = create_engine(Config.DATABASE_URI)
        self.metadata = MetaData()

        self.load_tables()

    def load_tables(self):
        for table_name in self.TABLES:
            setattr(
                self,
                table_name,
                Table(
                    table_name,
                    self.metadata,
                    autoload_with=self.engine,
                ),
            )

    def get_hotel_mapping_as_df(self):
        return pd.read_sql(sql=select(self.mapping_hotel), con=self.engine.connect())

    def get_room_mapping_as_df(self):
        return pd.read_sql(
            sql=select(
                self.accommodation_hotel_room.c.id,
                self.accommodation_hotel_room.c.hotel_id,
                self.accommodation_hotel_room.c.name,
                self.mapping_hotel_room.c.external_code,
                self.mapping_hotel_room.c.external_name,
            ).join_from(self.mapping_hotel_room, self.accommodation_hotel_room),
            con=self.engine.connect(),
        )

    def get_meal_mapping_as_df(self):
        return pd.read_sql(sql=select(self.definitions_meal_plan), con=self.engine.connect())

    def get_operator_mapping_as_df(self):
        return pd.read_sql(sql=select(self.mapping_operator), con=self.engine.connect())

    # def to_mapping_dict(self, df, key, value):
    #     if isinstance(key, str):
    #         return dict(zip(df[key], df[value]))
    #     else:
    #         key_tuples = df[key].apply(tuple, axis=1)
    #         return dict(zip(key_tuples, df[value]))
