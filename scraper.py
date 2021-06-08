from datetime import datetime

import requests


def is_date(raw_line):
    """
    Checks if date_string satisfies full month name, 0 padded day, full year (i.e. February 09, 2021)

    :param date_string: A date.
    :return: boolean, whether date_string is a date.
    """
    if len(raw_line) > 0:
        date_string = raw_line[0][:-1]  # there is a period at the end of dates.
        try:
            datetime.strptime(date_string, '%B %d, %Y')
            return True
        except ValueError:
            return False
    else:
        return False

print(is_date(['February 09, 2021']))