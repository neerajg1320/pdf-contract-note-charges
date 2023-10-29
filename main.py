from broker import *
from utils.debug import *

pd_set_options()

data_type = 'sample'

start_date = "2022-04-01"
if data_type == 'sample':
    end_date = "2023-01-31"
else:
    end_date = "2023-04-01"


def zerodha_post_process_fledger_dataframe(df):
    return df[df['Voucher Type'] == 'Book Voucher']


def zerodha_match_summary_dataframe(df):
    return df.shape == (11, 5) or df.shape == (11, 4)


zerodha_numeric_columns = ['Equity', 'Equity (T+1)', 'Futures and Options', 'NET TOTAL']


def zerodha_post_process_summary_dataframe(cnote_file_path, df):
    if df.shape[1] == 4:
        print("Fixed the df")
        df['Equity (T+1)'] = ""

    df = df[zerodha_numeric_columns]
    summary_df = df.map(get_decimal_or_blank_value)
    charges_df = pd.DataFrame(summary_df.values[1:-1,], columns=zerodha_numeric_columns)
    sum_series = charges_df.sum()
    charges_sum_df = sum_series.to_frame().transpose()
    return charges_sum_df


zerodha_broker = Broker('Zerodha',
                        input_path_prefix=f'data/{data_type}',
                        compute_path_prefix=f'compute/{data_type}',
                        fledger_date_column='Posting Date',
                        fledger_post_process_func=zerodha_post_process_fledger_dataframe,
                        charges_date_column='Date',
                        charges_numeric_columns=zerodha_numeric_columns,
                        summary_match_func=zerodha_match_summary_dataframe,
                        summary_post_process_func=zerodha_post_process_summary_dataframe
                        )

zerodha_broker.compute(start_date=start_date, end_date=end_date, dry_run=True)


axisdirect_numeric_columns = ['NCL-EQUITY', 'NCL F&O', 'NCL CDX', 'Total(Net)']


def axisdirect_post_process_fledger_dataframe(df):
    bool_df = df['Description1'].str.contains("Trade Bill")
    bool_df.fillna(False, inplace=True)
    return df[bool_df]


def axisdirect_match_charges_dataframe(df):
    return df.shape == (17, 6)


def axisdirect_post_process_charges_dataframe(cnote_file_path, df):
    df_print(df, active=False)

    charges_rows = [
        {'name': 'TaxableCharges', 'row': 4, 'aggregate': 'TaxableCharges'},
        {'name': 'CGST', 'row': 6, 'aggregate': 'GST'},
        {'name': 'SGST', 'row': 8, 'aggregate': 'GST'},
        {'name': 'IGST', 'row': 10, 'aggregate': 'GST'},
        {'name': 'UTGST', 'row': 12, 'aggregate': 'GST'},
        {'name': 'STT', 'row': 13, 'aggregate': 'STT'},
        {'name': 'StampDuty', 'row': 14, 'aggregate': 'StampDuty'},
    ]
    charges_row_indices = list(map(lambda x: x['row'], charges_rows))

    aggregate_map = {}

    df = df[axisdirect_numeric_columns]

    charges_df = df.iloc[charges_row_indices]
    charges_df = charges_df.map(get_decimal_or_blank_value)

    for index, (dfidx,row) in enumerate(charges_df.iterrows()):
        debug_log(row['NCL-EQUITY'], row['NCL F&O'], index, active=False)
        agg_key = charges_rows[index]['aggregate']
        if agg_key not in aggregate_map:
            aggregate_map[agg_key + "-EQ"] = 0
            aggregate_map[agg_key + "-FnO"] = 0
            aggregate_map[agg_key] = 0
        aggregate_map[agg_key + "-EQ"] += row['NCL-EQUITY']
        aggregate_map[agg_key + "-FnO"] += row['NCL F&O']
        aggregate_map[agg_key] += row['Total(Net)']

    debug_metadata(aggregate_map, active=False)

    # TBD: We should do the sum here
    sum_series = charges_df.sum()
    charges_sum_df = sum_series.to_frame().transpose()

    for key,value in aggregate_map.items():
        charges_sum_df[key] = value
    df_print(charges_sum_df, location=True, active=True)

    return charges_sum_df


axisdirect_broker = Broker('Axisdirect',
                           input_path_prefix=f'data/{data_type}',
                           compute_path_prefix=f'compute/{data_type}',
                           fledger_date_column='Trn Date',
                           fledger_date_format='%d-%b-%y',
                           fledger_post_process_func=axisdirect_post_process_fledger_dataframe,
                           charges_date_column='Date',
                           charges_numeric_columns=axisdirect_numeric_columns,
                           summary_match_func=axisdirect_match_charges_dataframe,
                           summary_post_process_func=axisdirect_post_process_charges_dataframe
                           )


# axisdirect_broker.read_ledger(start_date=start_date, end_date=end_date)
# axisdirect_broker.read_contract_notes(start_date=start_date, end_date=end_date, dry_run=False, max_count=2)
# axisdirect_broker.compute(start_date=start_date, end_date=end_date, dry_run=False)