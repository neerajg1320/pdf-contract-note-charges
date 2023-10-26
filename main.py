import os
import camelot
import pandas as pd
import numpy as np
from decimal import Decimal, InvalidOperation
import re


def get_dataframe_from_camelot_table(table):
    header_row = table.df.iloc[0]
    df_with_column_headers = pd.DataFrame(table.df.values[1:], columns=header_row)
    # print(df_with_column_headers)
    return df_with_column_headers


def get_charges_dataframe(tables):
    match_df = None
    for table in tables:
        df = get_dataframe_from_camelot_table(table)
        if df.shape == (11, 5):
            match_df = df

    return match_df


def get_decimal_or_blank_value(cell):
    value = np.NaN

    whitespace_regex = r"^\s*$"
    whitespace_match = re.match(whitespace_regex, cell)
    if whitespace_match is not None:
        value = 0
    else:
        bracketed_number_regex = r"^\(([\d.,]*)\)"
        match = re.match(bracketed_number_regex, cell)

        try:
            if match is not None:
                value = -Decimal(match.group(1))
            else:
                value = Decimal(cell)
        except InvalidOperation as e:
            print(e)

    return value


def convert_to_decimal(cell, ignore=False):
    try:
        new_cell = round(Decimal(cell), 4)
    except InvalidOperation as e:
        if ignore:
            new_cell = cell
        else:
            raise RuntimeError("Conversion failed for cell '{}'".format(cell))
            # debug_log("Conversion failed for cell '{}'".format(cell), location=False)

    return new_cell


def process_dataframe(input_df, columns=None):
    df = input_df
    if columns is not None:
        df = df[columns]

    # print(df)
    df = df.map(get_decimal_or_blank_value)
    # df = df.map(convert_to_decimal)
    # print(df)

    return df


def get_charges_aggregate_df_from_pdf(pdf_file_path):
    print(pdf_file_path)
    tables = camelot.read_pdf(pdf_file_path, pages="all")
    print(f"  Tables detected {len(tables)}")

    summary_df = get_charges_dataframe(tables)

    if summary_df is None:
        raise Exception("Charges table not found")

    # print(summary_df)
    # print(summary_df.dtypes)
    process_columns = ['Equity', 'Equity (T+1)', 'Futures and Options', 'NET TOTAL']
    # process_columns = ['NET TOTAL']

    summary_df[process_columns] = process_dataframe(summary_df, columns=process_columns)

    charges_df = pd.DataFrame(summary_df.values[1:-1, 1:], columns=process_columns)

    # print(charges_df)

    sum_series = charges_df.sum()
    sum_df = sum_series.to_frame().transpose()
    print( sum_df)

    return sum_df


def process_data(data_folder, max_count=0):
    count = 0
    aggregate_df = None
    for (root, dirs, files) in os.walk(data_folder):
        ordered_files = files.sort()
        for file in files:
            if max_count > 0 and count >= max_count:
                print(f"Max count {max_count} reached")
                break
            else:
                charges_sum_df = get_charges_aggregate_df_from_pdf(os.path.join(root, file))
                if aggregate_df is None:
                    aggregate_df = charges_sum_df
                else:
                    aggregate_df = pd.concat([aggregate_df, charges_sum_df], axis=0)
                count += 1

    if aggregate_df is not None:
        print(aggregate_df)


def pd_set_options():
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', 1000)


pd_set_options()
process_data('data', 0)
