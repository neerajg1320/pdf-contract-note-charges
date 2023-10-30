from broker import *
from utils.debug import *

pd_set_options()

data_type = 'sample'

start_date = "2022-04-01"
if data_type == 'sample':
    end_date = "2023-01-31"
    max_count = 3
else:
    end_date = "2023-04-01"
    max_count = 0


def zerodha_post_process_fledger_dataframe(df):
    return df[df['Voucher Type'] == 'Book Voucher']


def zerodha_match_summary_dataframe(df, **kwargs):
    if df.shape == (11, 5) or df.shape == (11, 4):
        return {'tag': 'Summary'}

    return None


zerodha_numeric_columns = ['Equity', 'Equity (T+1)', 'Futures and Options', 'NET TOTAL']


def zerodha_post_process_summary_dataframe(cnote_file_path, date, df, df_info):
    if df.shape[1] == 4:
        print("Fixed the df")
        df['Equity (T+1)'] = ""

    df = df[zerodha_numeric_columns]
    summary_df = df.map(convert_to_decimal_or_blank)
    charges_df = pd.DataFrame(summary_df.values[1:-1,], columns=zerodha_numeric_columns)
    sum_series = charges_df.sum()
    charges_sum_df = sum_series.to_frame().transpose()

    charges_sum_df = charges_sum_df.map(float)

    charges_sum_df['Date'] = date
    charges_sum_df['Document'] = cnote_file_path
    return charges_sum_df


zerodha_broker = Broker('Zerodha',
                        input_path_prefix=f'data/{data_type}',
                        compute_path_prefix=f'compute/{data_type}',
                        fledger_date_column='Posting Date',
                        fledger_post_process_func=zerodha_post_process_fledger_dataframe,
                        cnote_num_last_pages=2,
                        charges_date_column='Date',
                        summary_match_func=zerodha_match_summary_dataframe,
                        summary_post_process_func=zerodha_post_process_summary_dataframe
                        )

zerodha_broker.compute(start_date=start_date, end_date=end_date, dry_run=True, max_count=max_count)


axisdirect_numeric_columns = ['NCL-EQUITY', 'NCL F&O', 'NCL CDX', 'Total(Net)']


def axisdirect_post_process_fledger_dataframe(df):
    # Filter rows where Bill number is specified
    new_df = df[df["Bill No."].notnull()]

    return new_df


NUM_TRADE_COLUMNS = 12
NUM_CHARGES_COLUMNS = 6


def axisdirect_match_dataframe(df, page_num=None, **kwargs):
    if page_num is not None:
        debug_log(f"PageNum:{page_num} Detected table {df.shape} ", active=False)

    num_rows, num_columns = df.shape

    if num_rows == NUM_TRADE_COLUMNS:
        return {'tag': 'Trades'}

    # df_print(df, shape=True, active=True)

    # len_df = df.map(lambda cell: len(str(cell)))
    debug_log(df.columns, active=False)

    if num_columns == NUM_CHARGES_COLUMNS:
        return {'tag': 'Summary'}

    return None


def axisdirect_post_process_charges_dataframe(cnote_file_path, date, df, df_info):
    if df_info['tag'] != 'Summary':
        return pd.DataFrame()

    df_print(df, active=False)

    gross_row_index = 0
    net_row_index = 15

    charges_rows = [
        {'name': 'Brokerage', 'row': 1, 'aggregate': 'Brokerage'},
        {'name': 'ExchangeCharges', 'row': 2, 'aggregate': 'ExchangeCharges'},
        {'name': 'SEBIFees', 'row': 3, 'aggregate': 'SEBIFees'},
        # {'name': 'TaxableCharges', 'row': 4, 'aggregate': 'TaxableCharges'},
        {'name': 'CGST', 'row': 6, 'aggregate': 'GST'},
        {'name': 'SGST', 'row': 8, 'aggregate': 'GST'},
        {'name': 'IGST', 'row': 10, 'aggregate': 'GST'},
        {'name': 'UTGST', 'row': 12, 'aggregate': 'GST'},
        {'name': 'STT', 'row': 13, 'aggregate': 'STT'},
        {'name': 'StampDuty', 'row': 14, 'aggregate': 'StampDuty'},
    ]
    charges_row_indices = list(map(lambda x: x['row'], charges_rows))

    charges_aggregate_map = {}

    df = df[axisdirect_numeric_columns]

    charges_df = df.iloc[charges_row_indices]
    charges_df = charges_df.map(convert_to_decimal_or_blank)

    for index, (dfidx, row) in enumerate(charges_df.iterrows()):
        debug_log(row['NCL-EQUITY'], row['NCL F&O'], index, active=False)
        agg_key = charges_rows[index]['aggregate']

        if agg_key not in charges_aggregate_map:
            charges_aggregate_map[agg_key + "-EQ"] = 0
            charges_aggregate_map[agg_key + "-FnO"] = 0
            charges_aggregate_map[agg_key] = 0
        charges_aggregate_map[agg_key + "-EQ"] += row['NCL-EQUITY']
        charges_aggregate_map[agg_key + "-FnO"] += row['NCL F&O']
        charges_aggregate_map[agg_key] += row['Total(Net)']

    debug_metadata(charges_aggregate_map, active=False)

    # TBD: We should do the sum here
    sum_series = charges_df.sum()
    charges_sum_df = sum_series.to_frame().transpose()

    # Add the values in the charges_aggregate_map to dateframe
    for key,value in charges_aggregate_map.items():
        charges_sum_df[key] = value

    charges_sum_df['Gross-EQ'] = df.iloc[gross_row_index]['NCL-EQUITY']
    charges_sum_df['Gross-FnO'] = df.iloc[gross_row_index]['NCL F&O']
    charges_sum_df['Gross-Total'] = df.iloc[gross_row_index]['Total(Net)']

    charges_sum_df['Net-EQ'] = df.iloc[net_row_index]['NCL-EQUITY']
    charges_sum_df['Net-FnO'] = df.iloc[net_row_index]['NCL F&O']
    charges_sum_df['Net-Total'] = df.iloc[net_row_index]['Total(Net)']

    charges_sum_df = charges_sum_df.map(convert_to_decimal_or_blank)

    charges_sum_df['NetGrossDiff'] = charges_sum_df.loc[0, 'Net-Total'] - charges_sum_df.loc[0, 'Gross-Total']

    charges_sum_df['Status'] = charges_sum_df.loc[0, 'NetGrossDiff'] - charges_sum_df.loc[0, 'Total(Net)']

    df_print(charges_sum_df, location=True, active=False)

    # TBD: We need to fix this
    charges_sum_df = charges_sum_df.map(float)

    charges_sum_df['Date'] = date
    charges_sum_df['Document'] = cnote_file_path

    return charges_sum_df


axisdirect_broker = Broker('Axisdirect',
                           input_path_prefix=f'data/{data_type}',
                           compute_path_prefix=f'compute/{data_type}',
                           fledger_date_column='Trn Date',
                           fledger_date_format='%d-%b-%y',
                           fledger_post_process_func=axisdirect_post_process_fledger_dataframe,
                           cnote_num_last_pages=4,
                           charges_date_column='Date',
                           summary_match_func=axisdirect_match_dataframe,
                           summary_post_process_func=axisdirect_post_process_charges_dataframe
                           )


# axisdirect_broker.read_ledger(start_date=start_date, end_date=end_date)
# axisdirect_broker.read_contract_notes(start_date=start_date, end_date=end_date, dry_run=True, max_count=max_count)
axisdirect_broker.compute(start_date=start_date, end_date=end_date, dry_run=True)