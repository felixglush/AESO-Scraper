from collections import defaultdict

pool_participant_id = 'Pool Participant ID'
asset_type = 'Asset Type'
asset_id = 'Asset ID'
date = 'Date'
last_updated = 'Last Updated'
hour_closing_price = 'Hour Closing Price'
hour = 'Hour'
energy_unit = 'MWh'
peak_status = 'Peak Status'
name = 'Name'
hours_row = ['Hour 1', 'Hour 2', 'Hour 3', 'Hour 4', 'Hour 5', 'Hour 6', 'Hour 7', 'Hour 8', 'Hour 9', 'Hour 10',
             'Hour 11', 'Hour 12', 'Hour 13', 'Hour 14', 'Hour 15', 'Hour 16', 'Hour 17', 'Hour 18', 'Hour 19',
             'Hour 20', 'Hour 21', 'Hour 22', 'Hour 23', 'Hour 24']

# Choose columns
original_data_index = [pool_participant_id, asset_type, asset_id]
columns_in_scrape = original_data_index + hours_row
select_from_query = [asset_id, name, date, hour, energy_unit, peak_status]
select_columns = select_from_query + [last_updated]
sort_transform_by = [asset_id, date, hour]

# Filters
# desired_sites = ['941A', '941C'] # testing purposes with fake data
sites_to_extract_map = defaultdict(lambda: 'Unknown')
sites_to_extract_map['VQ6'] = 'Waterton'
sites_to_extract_map['ARD1'] = 'Ardenville'
sites_to_extract_map['BTR1'] = 'Blue Trail'
site_ids = sites_to_extract_map.keys()

'''
Peak hours: 7 am - 11 am
Hour Endings:
    Hr 1: 12:00 am - 12:59 am
    Hr 2: 1:00 am -1:59 am
    Hr 3: 2:00 am - 2:59 am
    ...
    Hr 7: 6:00 am - 6:59 am (Off-Peak)
    Hr 8: 7:00 am - 7:59 am (Peak)
    ...
    Hr 10: 9:00 - 9:59 am (Peak)
    Hr 11: 10:00 am - 10:59 am (Peak)
    Hr 12: 11:00 am - 11:59 am (Off-Peak)
'''
peak_hours_start = 8  # 7:00 am
peak_hours_end = 11  # 10:59 am

save_raw_dataframes_dir = 'pd_ready_reports/'
save_transformed_dataframes_dir = 'transformed_reports/'
save_raw_responses_dir = 'raw_reports/'
