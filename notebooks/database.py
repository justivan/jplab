import pandas as pd
from config import Config
from sqlalchemy import MetaData, Table, and_, create_engine, or_, select
from sqlalchemy.orm import Session


class Database:
    def __init__(self):
        self.engine = create_engine(Config.DATABASE_URI)
        self.metadata = MetaData()

        self.mapping_hotel = Table(
            "mapping_hotel",
            self.metadata,
            autoload_with=self.engine,
        )
        self.mapping_hotel_room = Table(
            "mapping_hotel_room",
            self.metadata,
            autoload_with=self.engine,
        )
        self.mapping_operator = Table(
            "mapping_operator",
            self.metadata,
            autoload_with=self.engine,
        )
        self.clients_operator = Table(
            "clients_operator",
            self.metadata,
            autoload_with=self.engine,
        )
        self.accommodation_hotel_room = Table(
            "accommodation_hotel_room",
            self.metadata,
            autoload_with=self.engine,
        )
        self.definitions_meal_plan = Table(
            "definitions_meal_plan",
            self.metadata,
            autoload_with=self.engine,
        )
        self.reservations_booking = Table(
            "reservations_booking",
            self.metadata,
            autoload_with=self.engine,
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
        return pd.read_sql(
            sql=select(self.definitions_meal_plan), con=self.engine.connect()
        )

    def get_operator_mapping_as_df(self):
        return pd.read_sql(sql=select(self.mapping_operator), con=self.engine.connect())