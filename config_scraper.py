# HOST PARAMETERS
host = 'http://ets.aeso.ca'
filename = '/ets_web/ip/Market/Reports/PublicSummaryAllReportServlet'

# the number of rows ahead closing prices and core data are after we see a date
closing_price_idx_jump_fwd = 5
core_data_idx_jump_fwd = 7
skip_dashes_idx = 3  # closing prices looks like ['-', '-', '-', actual data]

print_logs = True

rolling_window_days = 60
request_batch_size = 30
