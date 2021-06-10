import config
import scraper
import transformer
import requests
import config_scraper
from datetime import datetime, timedelta
import pandas as pd


def make_request(params):
    with requests.Session() as s:
        raw_response = s.get(config_scraper.host + config_scraper.filename, params=params)
        # save raw response to /raw_reports

        if params['contentType'] == 'csv':
            dates, closing_prices, core_data = scraper.process_csv(raw_response)
            return dates, closing_prices, core_data
        elif params['contentType'] == 'html':
            print('html contentType currently not supported')


def run_scrape_transform():
    """
    Maximum report size supported by AESO is 31 days.

    :return:
    """

    def create_date_batches():
        """

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
            if days_querying == 1:  # since 1 was subtracting above
                left_interval_date = right_interval_date
            else:
                left_interval_date = left_interval_date - timedelta(days=days_querying)

        return batches

    date_batches = create_date_batches()
    if config_scraper.print_logs:
        print('Date batches', date_batches)

    date_range_batches = []
    for batch in date_batches:
        beginDate, endDate = batch

        # make request and scrape
        dates, closing_prices, core_data = make_request(params={
            'beginDate': beginDate,
            'endDate': endDate,
            'contentType': config_scraper.content_type
        })

        # transform
        batch_result = transformer.process_all_dates(core_data, closing_prices, dates)
        date_range_batches.append(batch_result)

    result = pd.concat(date_range_batches, axis=0).reset_index(drop=True)
    result = result.sort_values(config.sort_transform_by)
    print('\n\nRESULT:')
    print(result)
    print(result.Date.unique())
    # result.to_csv()


if __name__ == '__main__':
    run_scrape_transform()
