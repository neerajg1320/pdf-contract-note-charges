from broker import *

pd_set_options()

data_type = 'main'

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


def zerodha_post_process_summary_dataframe(df):
    if df.shape[1] == 4:
        print("Fixed the df")
        df['Equity (T+1)'] = ""

    df = df[zerodha_numeric_columns]
    summary_df = df.map(get_decimal_or_blank_value)
    charges_df = pd.DataFrame(summary_df.values[1:-1,], columns=zerodha_numeric_columns)

    return charges_df


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

# zerodha_broker.compute(start_date=start_date, end_date=end_date, dry_run=True)


axisdirect_numeric_columns = ['NCL-EQUITY', 'NCL F&O', 'NCL CDX', 'Total(Net)']


def axisdirect_post_process_fledger_dataframe(df):
    bool_df = df['Description1'].str.contains("Trade Bill")
    bool_df.fillna(False, inplace=True)
    return df[bool_df]


def axisdirect_match_charges_dataframe(df):
    return df.shape == (17, 6)


def axisdirect_post_process_charges_dataframe(df):
    # print(df)

    charges_rows = [
        {'name': 'TaxableCharges', 'row': 4},
        {'name': 'CGST', 'row': 6},
        {'name': 'SGST', 'row': 8},
        {'name': 'IGST', 'row': 10},
        {'name': 'UTGST', 'row': 12},
        {'name': 'STT', 'row': 13},
        {'name': 'StampDuty', 'row': 14},
    ]
    row_indices = list(map(lambda x: x['row'], charges_rows))

    df = df[axisdirect_numeric_columns]
    df = df.iloc[row_indices]
    summary_df = df.map(get_decimal_or_blank_value)
    # print(summary_df)

    # charges_df = pd.DataFrame(summary_df.values[1:-2,], columns=axisdirect_numeric_columns)

    return summary_df


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
# axisdirect_broker.read_contract_notes(start_date=start_date, end_date=end_date, dry_run=True, max_count=0)
axisdirect_broker.compute(start_date=start_date, end_date=end_date, dry_run=False)