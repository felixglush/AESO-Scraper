import pandas as pd
import numpy as np
from datetime import datetime
from typing import List

# my code
import config

"""
    This file contains functions for transforming the data given lists of the data.
    
    Private functions at the top prepended with an underscore, public function at the bottom.
"""


def _load_data_from_list(day_data: pd.DataFrame, hour_values: list) -> pd.DataFrame:
    """
    Loads data from list into Pandas.


    Parameters:
        day_data (Pandas DataFrame): The day's data (everything below the multilevel header in AESO report).
        hour_values (List): the closing prices for a date


    Returns:
        A Pandas DataFrame version of the AESO Report.

    """

    # Load data into Pandas
    original_data = pd.DataFrame(data=day_data, columns=config.columns_in_scrape)
    original_data = original_data.set_index(config.original_data_index)

    # the result in the next line is exactly the table from the AESO report with the multi-level header
    original_data.columns = pd.MultiIndex.from_tuples(zip(config.hours_row, hour_values))

    return original_data


def _filter_data(data: pd.DataFrame, filters: dict) -> pd.DataFrame:
    """
    Filter data.


    Parameters:
        data (Pandas DataFrame: the AESO data to filter
        filters (Dictionary): defines filters


    Returns:
        Pandas DataFrame.
    """
    # Picks any from col 1 & 2 of the index, the chosen sites, and all of the non-index columns
    filtered_data = data.loc[(slice(None), slice(None), filters['sites']), :]

    return filtered_data


def _transform(data: pd.DataFrame, processing_date: str) -> pd.DataFrame:
    """
    Transforms data into a row wise format according to specifications.

    Parameters:
        data (Pandas DataFrame): AESO report data to transform
        processing_date (Date String): the date the data corresponds to

    Returns:
        A transformed Pandas DataFrame.
    """

    # This moves the closing prices and hours into columns
    # Reset index from original_data_index so that each index value appears in each row
    stacked_result = data.stack([0, 1]).reset_index()

    # Rename columns
    stacked_result.columns = [config.pool_participant_id, config.asset_type, config.asset_id,
                              config.hour, config.hour_closing_price, config.energy_unit]

    # Extract integer from Hour column and add Peak Status and Date columns
    stacked_result[config.hour] = stacked_result.apply(lambda row: row[config.hour].split(' ')[1], axis=1).astype(int)
    stacked_result[config.peak_status] = np.where((stacked_result[config.hour] >= config.peak_hours_start) &
                                                  (stacked_result[config.hour] <= config.peak_hours_end), 'Peak',
                                                  'Off-Peak')
    stacked_result[config.date] = processing_date
    stacked_result[config.last_updated] = datetime.today().strftime('%Y-%m-%d')

    # Select desired columns in given order then sort the rows
    target = stacked_result[config.select_columns]
    target = target.sort_values(config.sort_transform_by).reset_index(drop=True)

    return target


def _process_date(day_data: pd.DataFrame, process_date: str, hour_values: List) -> pd.DataFrame:
    """
    Performs tranformations for a single day. Returns a transformed dataframe for the date
    that was processed with schema defined by `select_columns`:
    ['Asset ID', 'Date', 'Hour', 'MWh', 'Peak Status', 'Last Updated']


    Parameters:
        day_data (Pandas DataFrame): The raw data for the current `process_date`.
        process_date (String): The current date that is being processed.
        hour_values (List): The closing prices for each hour


    Returns:
        Pandas DataFrame
    """

    loaded_data = _load_data_from_list(day_data, hour_values)

    filtered_data = _filter_data(loaded_data, filters={
        'sites': config.desired_sites
    })

    transformed_data = _transform(filtered_data, process_date)

    return transformed_data


def process_all_dates(all_data: pd.DataFrame, hour_values: List[List], dates: List) -> pd.DataFrame:
    """
    Takes in a list of data and dates (parsed from the AESO website) and returns a transformed Pandas DataFrame
    for all dates with schema defined by `select_columns`: ['Asset ID', 'Date', 'Hour', 'MWh', 'Peak Status', 'Last Updated']

    Parameters:
        all_data (Pandas DataFrame): 1 list per date, each of which consists of a list of rows.
        dates (List): list of dates
        hour_values (List of lists): the closing prices for each date

    Returns:
        Pandas DataFrame
    """

    resultant_data = pd.DataFrame(columns=config.select_columns)

    for i, processing_date in enumerate(dates):
        # Convert from column to row wise data
        target = _process_date(all_data[i], processing_date, hour_values[i])

        # Append the current processing_date's data to the result
        resultant_data = pd.concat([resultant_data, target], axis=0).reset_index(drop=True)

    return resultant_data
