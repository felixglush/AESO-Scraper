import csv
from datetime import datetime
import config_scraper
from config import original_data_index


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


def process_csv(raw_response):
    """
    Takes raw CSV response from the request to the AESO website and parses it into lists.

    Sensitive to format of response from AESO so assertion checks were placed in this function
    along with configurable indices in config_scraper.py

    Parameters:
        raw_response: Raw CSV string

    Returns:
         tuple of lists
    """

    decoded_response = raw_response.content.decode('utf-8')
    reader = csv.reader(decoded_response.splitlines(), delimiter=',')

    dates = []
    all_closing_prices = []  # 1 list per date consisting of 1 row for that day
    all_core_data = []  # 1 list per date consisting of all rows for that day

    closing_price_idx = -1
    core_data_start_idx = -1
    reading_core_data = False  # whether we are currently processing core energy data lines

    day_core_data = []  # used to append rows of core data for the date we're currently processing

    for i, row in enumerate(reader):
        if i == closing_price_idx:  # reached the line that contains closing prices
            current_closing_prices = row[config_scraper.skip_dashes_idx:]

            ''' 
            Apparently march 14 is missing hour 2 closing price (found during testing different rolling windows)
            assert len(current_closing_prices) == 24, 'Need 1 closing price for each hour'
            '''
            # if len(current_closing_prices) != 24:
            #     print('Check the date currently being processed on AESO. It appears to be missing an hour(s).')
            all_closing_prices.append(current_closing_prices)
        elif i == core_data_start_idx:  # reached the line that has the first row of core data
            reading_core_data = True

        if len(row) == 0 and reading_core_data:
            # we've run out of core data to process. There is a blank line ([]) after core data ends.
            reading_core_data = False
            all_core_data.append(day_core_data)
            day_core_data = []
            continue
        elif reading_core_data:  # we're still reading core data
            # if len(row) != 24 + len(original_data_index):
                # previously assert statement: 'Core data row not equal to 24 values for each hour and 1 for each of ' + original_data_index
                # print('Check the date currently being processed on AESO. It appears to be missing an hour(s).')
            day_core_data.append(row)

        if is_date(row):
            # this means the closing prices start in 5 indices and the
            # energy data starts in 7 indices
            if config_scraper.print_logs:
                print('Currently scraping', row[0])
            dates.append(row[0][:-1])  # the :-1 drops the period at the end of date

            closing_price_idx = i + config_scraper.closing_price_idx_jump_fwd
            core_data_start_idx = i + config_scraper.core_data_idx_jump_fwd

    return dates, all_closing_prices, all_core_data
