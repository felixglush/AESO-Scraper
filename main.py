import config
import scraper
import transformer
import requests
import config_scraper
from datetime import datetime, timedelta
import pandas as pd


def run_scrape_transform() -> pd.DataFrame:
    """
        Main entry point. Runs scraper and transforms the data.
    """

    def create_date_batches():
        """
        Maximum report size supported by AESO is 31 days.

        :return: a tuple of start and end date for splitting requests
        """

        days_to_query_left = config_scraper.rolling_window_days
        batches = []  # list of tuples

        right_interval_date = datetime.today() - timedelta(days=1)  # the AESO website doesn't work for the current day
        left_interval_date = right_interval_date - timedelta(days=config_scraper.request_batch_size - 1)

        while days_to_query_left:
            days_querying = min(config_scraper.request_batch_size, days_to_query_left)
            days_to_query_left -= days_querying

            batches.append((left_interval_date.strftime('%m%d%Y'), right_interval_date.strftime('%m%d%Y')))

            right_interval_date = left_interval_date - timedelta(days=1)
            if days_querying == 1:  # since 1 was subtracted above
                left_interval_date = right_interval_date
            else:
                left_interval_date = left_interval_date - timedelta(days=days_querying)

        return batches

    date_batches = create_date_batches()
    if config_scraper.print_logs:
        print('Date batches', date_batches)

    date_range_batches = []
    with requests.Session() as session_obj:
        for batch in date_batches:
            beginDate, endDate = batch

            # make request and scrape
            dates, closing_prices, core_data = scraper.request_scrape(session_obj,
                                                                      params={
                                                                          'beginDate': beginDate,
                                                                          'endDate': endDate,
                                                                          'contentType': config_scraper.content_type
                                                                      })

            # transform
            batch_result = transformer.process_all_dates(core_data, closing_prices, dates)
            date_range_batches.append(batch_result)

    final_report = pd.concat(date_range_batches, axis=0).reset_index(drop=True)
    final_report = transformer.sort_data(final_report)

    return final_report


if __name__ == '__main__':
    report = run_scrape_transform()

    print('\n\nRESULT:')
    pd.set_option('display.max_columns', None)  # prints all columns
    print(report)
