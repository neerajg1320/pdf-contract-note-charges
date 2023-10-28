from broker import *

pd_set_options()

data_type = 'sample'

start_date = "2022-04-01"
if data_type == 'sample':
    end_date = "2023-01-31"
else:
    end_date = "2023-04-01"


zerodha_broker = Broker('Zerodha',
                        input_path_prefix=f'data/{data_type}',
                        compute_path_prefix=f'compute/{data_type}',
                        fledger_date_column='Posting Date',
                        charges_date_column='Date',
                        charges_numeric_columns=['Equity', 'Equity (T+1)', 'Futures and Options', 'NET TOTAL'],
                        )

zerodha_broker.compute(start_date=start_date, end_date=end_date, dry_run=True)

