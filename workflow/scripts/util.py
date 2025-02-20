""" various utility functions. """
import os
import logging
from datetime import datetime, timedelta
import requests

LOG_FORMAT = "[%(asctime)s] %(levelname)s - %(message)s"
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

def str_to_date(date_str, fmt="%Y-%m-%d"):
    """
    Convert a string to a date object.
    :param date_str: The date string.
    :param format: The format of the date string.
    :return: The date object.
    """
    return datetime.strptime(date_str, fmt).date()


def now():
    """
    Get the current date and time.
    :return: The current date and time
    """
    return datetime.now()


def setup_logger(log_level="INFO"):
    """
    Setup the logger.
    :param log_level: "INFO", "DEBUG", "WARNING", "ERROR", "CRITICAL"
    :return: The logger object.
    """
    log = logging.getLogger("default")

    if "DEBUG" in os.environ:
        log_level = "DEBUG"

    log.setLevel(log_level)
    formatter = logging.Formatter(LOG_FORMAT, DATE_FORMAT)
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    log.addHandler(console_handler)

    return log


def download_http_file(url, file_path):
    """
    Download a file from a URL.
    :param url: The URL of the file.
    :param file_path: The path to save the file.
    """
    response = requests.get(url, timeout=10)

    try:
        with open(file_path, 'wb') as file:
            file.write(response.content)
            logging.debug("File downloaded: %s", file_path)
    except Exception as e:
        raise Exception(f"Error downloading file: {e}")


def generate_date_ranges(start_date, end_date, interval):
    """
    Generate a list of date ranges based on the specified interval.
    :param start_date: The start date.
    :param end_date: The end date.
    :param interval: The interval for creating date ranges.
    :return: A list of tuples with start and end dates for each interval.
    """
    date_ranges = []
    current_date = start_date

    if interval == 'm':
        while current_date <= end_date:
            next_month = datetime(current_date.year + (current_date.month // 12),
                                  (current_date.month % 12) + 1, 1).date()
            month_end = min(next_month - timedelta(days=1), end_date)
            date_ranges.append([current_date, month_end])
            current_date = next_month

    elif interval == 'w':
        while current_date <= end_date:
            week_start = current_date - timedelta(days=current_date.weekday())
            week_end = min(week_start + timedelta(days=6), end_date)
            date_ranges.append([week_start, week_end])
            current_date = week_end + timedelta(days=1)

    elif interval == 'd':
        while current_date <= end_date:
            date_ranges.append([current_date, current_date])
            current_date += timedelta(days=1)

    return date_ranges
