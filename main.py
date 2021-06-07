import re

import requests
import csv
import datetime
import pandas as pd
from bs4 import BeautifulSoup

# HOST PARAMETERS
host = 'http://ets.aeso.ca'
filename = '/ets_web/ip/Market/Reports/PublicSummaryAllReportServlet'

# HEADERS
# Date format: MMDDYYYY
beginDate = '02092021'
endDate = '02092021'
contentType = 'csv'  # csv, html

sites_to_extract = {'VQ6': 'Waterton',
                    'ARD1': 'Ardenville',
                    'BTR1': 'Blue Trail'}

peak_hours = list(range(7, 10))  # 7:00 am to 10:59 am

report_name_idx = 0
report_date_idx = 3
report_start_date = 5
report_index = 7
report_hours = 8
report_closing_price = 10
report_data_start = 12


def is_date(date_string):
    """
    Checks if date_string satisfies full month name, 0 padded day, full year (i.e. February 09, 2021)

    :param date_string: A date.
    :return: boolean, whether date_string is a date.
    """
    try:
        datetime.datetime.strptime(date_string, '%B %d, %Y')
        return True
    except ValueError:
        return False


if __name__ == '__main__':
    params = {
        'beginDate': beginDate,
        'endDate': endDate,
        'contentType': contentType
    }

    with requests.Session() as s:
        response = s.get(host + filename, params=params)
        # save raw response to /raw_reports

        # prepare a pandas object for each date and save it to pd_ready_reports
        if contentType == 'csv':
            decoded_response = response.content.decode('utf-8')
            reader = csv.reader(decoded_response.splitlines(), delimiter=',')
            for i, row in enumerate(reader):
                # make a dataframe out of the identifiers
                if i == report_index:
                    data = {
                        'Pool Participant ID': [],
                        'Asset Type': [],
                        'Asset ID': []
                    }
                    # pd.DataFrame(data=)


                # make a dataframe out of the core data
                # unstack and make a row in the header for the closing price.

                    # if i == report_name_idx or i == report_date_idx:
                    #     continue
                    # if is_date(row):
                    #     pass
                        # create a new pandas object
                print(i, row)

        elif contentType == 'html':
            soup = BeautifulSoup(response.text, 'lxml')
            # for t in soup.find_all(text=re.compile())

            for t in soup.find_all(text=re.compile('Pool Participant ID')):
                table_html = t.findParent('table')
                print(table_html)
                table_headers = table_html.find_all('th')
                table_rows = table_html.find_all('tr')
                # print(table_rows)
                # print(table_headers)
