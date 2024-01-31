import logging
import urllib
from pathlib import Path

import requests
from config import Config
from requests_ntlm import HttpNtlmAuth

BASE_DIR = Path(".").resolve()


class BookingData:
    """
    Represents booking data for generating SSRS reports.

    Parameters:
        - destination (str): The destination for the booking data.
        - date_from (str): The start date for the specified option.
        - date_to (str): The end date for the specified option.
        - option (int): The option to choose the type of date.

    Options:
        - 1: Arrival date
        - 2: In-house date
        - 3: Max processed date

    Usage:
        Specify the destination, date range, and option to create a BookingData instance.
    """

    def __init__(self, destination, date_from, date_to, option):
        """
        Initialize a BookingData instance.

        Args:
            destination (str): The destination for the booking data.
            date_from (str): The start date for the specified option.
            date_to (str): The end date for the specified option.
            option (int): The option to choose the type of date.

        Raises:
            ValueError: If the option is not between 1 and 3.
        """
        self.ssrs_url = Config.SSRS_BASE_URL + destination + " Reports/Reservations/Bookings Data"
        self.ssrs_usr = Config.SSRS_USERNAME
        self.ssrs_pwd = Config.SSRS_PASSWORD

        if option == 1:
            self.payload = [
                ("from", date_from),
                ("to", date_to),
            ]
        elif option == 2:
            self.payload = [
                ("d1", date_from),
                ("d2", date_to),
            ]
        elif option == 3:
            self.payload = [
                ("MaxProcessDate_from", date_from),
                ("MaxProcessDate_to", date_to),
            ]
        else:
            raise ValueError("Option should be between 1 and 3.")

        self.payload.extend(
            [
                ("ReportParameter1", True),
                ("RefIDs:isnull", True),
                ("rs:ParameterLanguage", ""),
                ("rs:Command", "Render"),
                ("rs:Format", "CSV"),
                ("rc:ItemPath", "table1"),
            ]
        )

        self.params = urllib.parse.urlencode(self.payload, quote_via=urllib.parse.quote)

    def get(self):
        """
        Retrieve booking data from the SSRS server.

        Returns:
            str or None: The booking data as a string if available, or None if there's no new data.

        Raises:
            requests.exceptions.HTTPError: If an HTTP error occurs during the request.
            requests.exceptions.RequestException: If a general request error occurs.
        """
        logging.basicConfig(
            filename=BASE_DIR / "logs" / "get_booking_data.log",
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s: %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )

        try:
            response = requests.get(
                self.ssrs_url,
                params=self.params,
                stream=True,
                auth=HttpNtlmAuth(self.ssrs_usr, self.ssrs_pwd),
            )

            response.raise_for_status()

            data = response.content.decode("utf8")

            if len(data) > 424:
                logging.info(f"Request to {self.ssrs_url} was successful.")
                return data
            else:
                logging.warning(f"No new data available for: {self.ssrs_url.split('?')[1]}")
        except requests.exceptions.HTTPError as e:
            logging.error(f"HTTP error occurred: {str(e)}")
            logging.exception("Full traceback:")
            raise  # re-raise the exception for the caller to handle if needed
        except requests.exceptions.RequestException as e:
            logging.error(f"An error occurred while making the request: {str(e)}")
            logging.exception("Full traceback:")
            raise  # re-raise the exception for the caller to handle if needed

        return None


class HotelData:
    def __init__(self, destination):
        self.ssrs_url = Config.SSRS_BASE_URL + destination + " Reports/Main Data/HotelList"
        self.ssrs_usr = Config.SSRS_USERNAME
        self.ssrs_pwd = Config.SSRS_PASSWORD
        self.payload = [
            ("Active", True),
            ("rs:Command", "Render"),
            ("rs:Format", "CSV"),
            ("rc:ItemPath", "table1"),
        ]

        self.params = urllib.parse.urlencode(self.payload, quote_via=urllib.parse.quote)

    def get(self):
        response = requests.get(
            self.ssrs_url,
            params=self.params,
            stream=True,
            auth=HttpNtlmAuth(self.ssrs_usr, self.ssrs_pwd),
        )

        if response.status_code == 200:
            data = response.content.decode("utf8")
            return data
        return None
