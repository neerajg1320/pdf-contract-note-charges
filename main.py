import os
import camelot
from pypdf import PdfReader
import pandas as pd
import numpy as np
from decimal import Decimal, InvalidOperation
import re



whitespace_regex = r"^\s*$"
bracketed_number_regex = r"^\(([\d.,]*)\)"
date_regex = r"\d{4}-\d{2}-\d{2}"


def get_date_from_string(input_str):
    match = re.search(date_regex, input_str)
    if match is not None:
        return match.group(0)
    return None


def get_dataframe_from_camelot_table(table):
    header_row = table.df.iloc[0]
    header_row = header_row.map(lambda cell: cell.replace("\n", " "))
    df_with_column_headers = pd.DataFrame(table.df.values[1:], columns=header_row)
    # print(df_with_column_headers)
    return df_with_column_headers


def get_charges_dataframe(tables):
    match_df = None
    for table in tables:
        df = get_dataframe_from_camelot_table(table)
        if df.shape == (11, 5) or df.shape == (11, 4):
            match_df = df

    return match_df


def get_decimal_or_blank_value(cell):
    value = np.NaN

    whitespace_match = re.match(whitespace_regex, cell)
    if whitespace_match is not None:
        value = 0
    else:
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


def get_pdf_number_of_pages(pdf_file_path):
    reader = PdfReader(pdf_file_path)
    return len(reader.pages)


def get_charges_aggregate_df_from_pdf(pdf_file_path):
    # print(pdf_file_path)

    num_pages = get_pdf_number_of_pages(pdf_file_path)
    # print(f"{pdf_file_path}: Number of pages:", num_pages)

    last_two_pages = f"{num_pages-1},{num_pages}"
    tables = camelot.read_pdf(pdf_file_path, pages=last_two_pages)
    print(f"{pdf_file_path}:  {len(tables)} Tables detected on pages:{last_two_pages} ")

    summary_df = get_charges_dataframe(tables)
    if summary_df.shape[1] == 4:
        summary_df['Equity (T+1)'] = ""

    if summary_df is None:
        raise Exception("Charges table not found")

    process_columns = ['Equity', 'Equity (T+1)', 'Futures and Options', 'NET TOTAL']

    summary_df = summary_df[['', 'Equity', 'Equity (T+1)', 'Futures and Options', 'NET TOTAL']]

    # print(summary_df)
    # print(summary_df.dtypes)

    summary_df[process_columns] = process_dataframe(summary_df, columns=process_columns)

    charges_df = pd.DataFrame(summary_df.values[1:-1, 1:], columns=process_columns)

    # print(charges_df)

    sum_series = charges_df.sum()
    sum_df = sum_series.to_frame().transpose()
    print(sum_df)

    return sum_df


debug_process = False
def process_contractnotes_folder(data_folder, *, start_date=None, end_date=None, max_count=0):
    count = 0
    aggregate_df = None
    for (root, dirs, files) in os.walk(data_folder):
        files.sort()
        for file in files:
            if max_count > 0 and count >= max_count:
                print(f"Max count {max_count} reached")
                break

            date = get_date_from_string(file)
            if date is None:
                if debug_process:
                    print(f"Could not find date in file '{file}'")
                continue

            if start_date:
                if date < start_date:
                    if debug_process:
                        print(f"date {date} is earlier than start_date {start_date}")
                    continue

            if end_date:
                if date >= end_date:
                    if debug_process:
                        print(f"date {date} is later than end_date {end_date}")
                    continue

            pdf_file_path = os.path.join(root, file)

            try:
                charges_sum_df = get_charges_aggregate_df_from_pdf(pdf_file_path)

                charges_sum_df['Date'] = date
                charges_sum_df = charges_sum_df[['Date', 'Equity', 'Equity (T+1)', 'Futures and Options', 'NET TOTAL']]

                if aggregate_df is None:
                    aggregate_df = charges_sum_df
                else:
                    aggregate_df = pd.concat([aggregate_df, charges_sum_df], axis=0)
                count += 1
            except KeyError as e:
                print(type(e).__name__, e)
                # print(f"Error processing contract note for date {date}")

    if aggregate_df is not None:
        print(aggregate_df)

    return aggregate_df


def process_financialledger_file(data_file, *, start_date=None, end_date=None, max_count=0):
    count = 0
    aggregate_df = None

    fledger_df = pd.read_excel(data_file)
    tradeentry_df = fledger_df[fledger_df['Voucher Type'] == 'Book Voucher']
    # print(tradeentry_df)
    # print(tradeentry_df.shape)

    return tradeentry_df


def reconcile_charges_and_ledger(ledger_df, charges_df):
    # print(ledger_df)
    # print(charges_df)

    # We will do an outer join
    merged_df = ledger_df.merge(charges_df, left_on='Posting Date', right_on='Date', how='outer')
    # print(merged_df)
    return merged_df


def find_unmatched(joined_df):
    mismatch_df = joined_df[joined_df['Posting Date'] != joined_df['Date']]
    # print(mismatch_df)
    return mismatch_df


def pd_set_options():
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', 1000)


def generate_report_from_unmatched(unmatched_df, *, on=None, left_on=None, right_on=None, left_report=None, right_report=None):
    if len(unmatched_df) > 0:
        for index,row in unmatched_df.iterrows():
            # print(type(row))
            # print(row)
            print('left: ', row[left_on], type(row[left_on]), 'right: ', row[left_on], type(row[left_on]))
            if pd.isna(row[left_on]):
                print(f"{left_on} is missing in {left_report}")
            if pd.isna(row[right_on]):
                print(f"{right_on} is missing in {right_report}")


def create_output_file(charges_df, output_file_path):
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    if charges_df is not None:
        charges_df.to_excel(output_file_path)


pd_set_options()

data_type = 'main'

output_folder = f'output/{data_type}'


output_format = 'xlsx'
charges_file_name = f'charges.{output_format}'
charges_file_path = os.path.join(output_folder, charges_file_name)

financialledger_file_path = f'data/{data_type}/FinancialLedger/Zerodha/Zerodha_FinancialLedger_Transactions.xlsx'
contractnotes_folder = f'data/{data_type}/ContractNotes/Zerodha'

account_provider_name = "Zerodha"

charges_document_name = f'{account_provider_name} Charges Aggregate'
financialledger_document_name = f'{account_provider_name} Financial Ledger'


start_date = "2022-04-01"
if data_type == 'sample':
    end_date = "2022-04-10"
else:
    end_date = "2023-04-01"

tradeledger_df = process_financialledger_file(financialledger_file_path, start_date=start_date, end_date=end_date)

if not os.path.exists(charges_file_path):
    charges_aggregate_df = process_contractnotes_folder(contractnotes_folder, start_date=start_date, end_date=end_date)
    create_output_file(charges_aggregate_df, charges_file_path)
else:
    charges_aggregate_df = pd.read_excel(charges_file_path)

reconciled_df = reconcile_charges_and_ledger(tradeledger_df, charges_aggregate_df)

print('Missing Entries')
unmatched_df = find_unmatched(reconciled_df)

generate_report_from_unmatched(unmatched_df, left_on='Posting Date', right_on='Date', left_report=financialledger_document_name, right_report=financialledger_document_name)

