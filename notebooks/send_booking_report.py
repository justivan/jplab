from datetime import date, datetime, timedelta

import pandas as pd
from database import Database
from email_sender import EmailSender
from excel_writer import ExcelWriter
from sqlalchemy import func, select

db = Database()

select_bookings = (
    select(
        db.gwg_reservation.c.status,
        db.users_user.c.name.label("purchase_manager"),
        db.clients_operator.c.short_name.label("operator_name"),
        db.gwg_reservation.c.ref_id,
        db.gwg_reservation.c.res_id,
        db.gwg_reservation.c.bkg_ref,
        db.mapping_hotel.c.external_name.label("hotel_name"),
        db.gwg_reservation.c.guest_name,
        db.gwg_reservation.c.sales_date,
        db.gwg_reservation.c.in_date,
        db.gwg_reservation.c.out_date,
        db.gwg_reservation.c.room_type,
        db.gwg_reservation.c.meal,
        db.gwg_reservation.c.adult,
        db.gwg_reservation.c.child,
        db.gwg_reservation.c.days,
        db.gwg_reservation.c.create_date,
    )
    .outerjoin(
        db.mapping_hotel,
        db.gwg_reservation.c.hotel_id == db.mapping_hotel.c.external_code,
    )
    .outerjoin(
        db.mapping_operator,
        db.gwg_reservation.c.operator_id == db.mapping_operator.c.external_code,
    )
    .outerjoin(db.accommodation_hotel)
    .outerjoin(db.accommodation_purchase_manager)
    .outerjoin(db.users_user)
    .outerjoin(db.definitions_area)
    .outerjoin(db.definitions_region)
    .outerjoin(db.clients_operator)
    .where(
        # db.definitions_region.c.country_code == "AE",
        db.clients_operator.c.category == "IC",
        db.mapping_operator.c.external_code != 1107,
    )
    .order_by(
        db.mapping_hotel.c.external_name,
        db.gwg_reservation.c.in_date,
        db.gwg_reservation.c.ref_id,
    )
)

current_date = datetime.now()

export = []

for destination in ["AE", "OM", "SA"]:
    outfile = (
        f'GWG_{destination}_RESLIST_{current_date.strftime("%Y_%m_%d_%H%M%S")}.xlsx'
    )

    new = pd.read_sql(
        sql=select_bookings.where(
            func.date(db.gwg_reservation.c.create_date) == current_date.date(),
            db.gwg_reservation.c.status != "Can",
            db.definitions_region.c.country_code == destination,
        ),
        con=db.engine.connect(),
    )

    ame = pd.read_sql(
        sql=select_bookings.where(
            func.date(db.gwg_reservation.c.last_modified_date) == current_date.date(),
            func.date(db.gwg_reservation.c.create_date) != current_date.date(),
            db.gwg_reservation.c.status != "Can",
            db.definitions_region.c.country_code == destination,
        ),
        con=db.engine.connect(),
    )

    can = pd.read_sql(
        sql=select_bookings.where(
            func.date(db.gwg_reservation.c.last_modified_date) == current_date.date(),
            db.gwg_reservation.c.status == "Can",
            db.definitions_region.c.country_code == destination,
        ),
        con=db.engine.connect(),
    )

    new.name = "new"
    ame.name = "ame"
    can.name = "can"

    excel_writer = ExcelWriter([new, ame, can], outfile)
    excel_writer.write_to_excel(date_column=[8, 9, 10, 16])
    excel_writer.writer.close()

    export.append(outfile)

sender = EmailSender(
    subject=f"Yesterday's Booking Intake - {(current_date - timedelta(1)).strftime('%d %b %Y')}",
    to=[
        "ivan.orara@meetingpointuae.com",
    ],
)

sender.send_email(export)