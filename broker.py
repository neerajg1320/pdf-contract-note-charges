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


def get_charges_dataframe(tables, match_func):
    if match_func is None:
        raise RuntimeError("match_func parameter is mandatory")

    match_df = None
    for table in tables:
        df = get_dataframe_from_camelot_table(table)
        if match_func(df):
            match_df = df

    return match_df



def process_dataframe(input_df, columns=None):
    df = input_df
    if columns is not None:
        df = df[columns]

    df = df.map(get_decimal_or_blank_value)

    return df


def get_pdf_number_of_pages(pdf_file_path):
    reader = PdfReader(pdf_file_path)
    return len(reader.pages)


def get_charges_aggregate_df_from_pdf(pdf_file_path, numeric_columns=None, charges_match_func=None, charges_post_process_func=None):
    if pdf_file_path is None:
        raise RuntimeError(f"pdf_file_path is not provided")

    if numeric_columns is None:
        raise RuntimeError(f"numeric_columns is not provided")

    if charges_match_func is None:
        raise RuntimeError(f"charges_match_func is not provided")

    # print(pdf_file_path)

    num_pages = get_pdf_number_of_pages(pdf_file_path)
    # print(f"{pdf_file_path}: Number of pages:", num_pages)

    last_two_pages = f"{num_pages-1},{num_pages}"
    tables = camelot.read_pdf(pdf_file_path, pages=last_two_pages)
    print(f"{pdf_file_path}:  {len(tables)} Tables detected on pages:{last_two_pages} ")

    summary_df = get_charges_dataframe(tables, charges_match_func)

    if charges_post_process_func is not None:
        summary_df = charges_post_process_func(summary_df)

    if summary_df is None:
        raise Exception("Charges table not found")

    summary_columns = ['']
    summary_columns.extend(numeric_columns)

    summary_df = summary_df[summary_columns]

    # print(summary_df)
    # print(summary_df.dtypes)

    summary_df[numeric_columns] = process_dataframe(summary_df, columns=numeric_columns)

    charges_df = pd.DataFrame(summary_df.values[1:-1, 1:], columns=numeric_columns)

    # print(charges_df)

    sum_series = charges_df.sum()
    sum_df = sum_series.to_frame().transpose()
    print(sum_df)

    return sum_df


debug_process = False
def process_contractnotes_folder(cnotes_folder_path, *,
                                 charges_aggregate_file_path=None,
                                 charges_match_func=None,
                                 charges_post_process_func=None,
                                 date_column='Date',
                                 numeric_columns=None,
                                 start_date=None,
                                 end_date=None,
                                 max_count=0,
                                 dry_run=False):
    if not os.path.exists(cnotes_folder_path):
        raise RuntimeError(f"folder '{cnotes_folder_path}' does not exist")

    count = 0

    aggregate_df = pd.DataFrame()
    if charges_aggregate_file_path is not None:
        if os.path.exists(charges_aggregate_file_path):
            print(f"Reading charges aggregate file '{charges_aggregate_file_path}'")
            aggregate_df = pd.read_excel(charges_aggregate_file_path)
        else:
            aggregate_df = pd.DataFrame()

    print(f"Traversing contract notes folder '{cnotes_folder_path}'")
    for (root, dirs, files) in os.walk(cnotes_folder_path):
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

            # We ignore the files which are already present
            if len(aggregate_df):
                if len(aggregate_df[aggregate_df[date_column] == date]):
                    continue

            pdf_file_path = os.path.join(root, file)

            try:
                charges_sum_df = get_charges_aggregate_df_from_pdf(pdf_file_path,
                                                                   numeric_columns=numeric_columns,
                                                                   charges_match_func=charges_match_func,
                                                                   charges_post_process_func=charges_post_process_func
                                                                   )

                charges_sum_df['Date'] = date

                charges_columns = [date_column]
                charges_columns.extend(numeric_columns)

                charges_sum_df = charges_sum_df[charges_columns]

                aggregate_df = pd.concat([aggregate_df, charges_sum_df], axis=0)
                count += 1
            except KeyError as e:
                print(type(e).__name__, e)
                # print(f"Error processing contract note for date {date}")

    if aggregate_df is not None:
        print(aggregate_df)

    if count > 0:
        # We convert the decimal columns to float
        aggregate_df[numeric_columns] = aggregate_df[numeric_columns].map(float)
        create_output_file(aggregate_df, charges_aggregate_file_path, dry_run=dry_run)

    return aggregate_df


def process_financialledger_file(data_file, *, start_date=None, end_date=None, max_count=0):
    fledger_df = pd.read_excel(data_file)
    tradeentry_df = fledger_df[fledger_df['Voucher Type'] == 'Book Voucher']
    # print(tradeentry_df)
    # print(tradeentry_df.shape)

    return tradeentry_df


def reconcile_charges_and_ledger(ledger_df, charges_df, *, ledger_date_column='Date', charges_date_column='Date'):
    # print(ledger_df)
    # print(charges_df)

    # We will do an outer join
    merged_df = ledger_df.merge(charges_df, left_on=ledger_date_column, right_on=charges_date_column, how='outer')
    # print(merged_df)
    return merged_df


def find_unmatched(joined_df, *, ledger_date_column='Date', charges_date_column='Date'):
    mismatch_df = joined_df[joined_df[ledger_date_column] != joined_df[charges_date_column]]
    # print(mismatch_df)
    return mismatch_df


def pd_set_options():
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', 1000)


def generate_report_from_unmatched(unmatched_df, *, on=None, left_on=None, right_on=None, left_report=None, right_report=None):
    if len(unmatched_df) > 0:
        for index,row in unmatched_df.iterrows():
            # We use the value for right in left and vice-versa since it is missing
            if pd.isna(row[left_on]):
                print(f"Report '{left_report}' has missing entry for date {row[right_on]}")
            if pd.isna(row[right_on]):
                print(f"Report '{right_report}' has missing entry for date {row[left_on]}")


def create_output_file(charges_df, output_file_path, dry_run=False):
    if dry_run:
        return

    output_folder = os.path.dirname(output_file_path)
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    if not dry_run:
        if charges_df is not None:
            # print(charges_df.map(type))
            charges_df.to_excel(output_file_path)



class Provider:
    def __init__(self, name, type):
        self.name = name
        self.type = type


class Broker(Provider):
    output_format = 'xlsx'

    def __init__(self, name, *,
                 input_path_prefix='data',
                 compute_path_prefix='compute',
                 fledger_date_column='Date',
                 charges_date_column='Date',
                 charges_numeric_columns=None,
                 charges_match_func=None,
                 charges_post_process_func=None):
        super(Broker, self).__init__(name, "Broker")
        self.fledger_path = os.path.join(input_path_prefix, f'FinancialLedger/{self.name}/{self.name}_FinancialLedger_Transactions.xlsx')
        self.cnote_folder_path = os.path.join(input_path_prefix, f'ContractNotes/{self.name}')
        self.charges_file_path = os.path.join(compute_path_prefix, self.name, f'charges.{self.output_format}')
        self.charges_match_func = charges_match_func
        self.charges_post_process_func = charges_post_process_func
        self.fledger_date_column = fledger_date_column
        self.charges_date_column = charges_date_column
        self.charges_numeric_columns = charges_numeric_columns

        self.tradeledger_df = None
        self.charges_aggregate_df = None
        self.reconciled_df = None
        self.unnatched_df = None

    def compute(self, start_date=None, end_date=None, dry_run=False):
        charges_document_name = f'{self.name} Charges Aggregate'
        financialledger_document_name = f'{self.name} Financial Ledger'

        self.tradeledger_df = process_financialledger_file(self.fledger_path,
                                                      start_date=start_date,
                                                      end_date=end_date)

        self.charges_aggregate_df = process_contractnotes_folder(self.cnote_folder_path,
                                                                 charges_aggregate_file_path=self.charges_file_path,
                                                                 charges_match_func=self.charges_match_func,
                                                                 charges_post_process_func=self.charges_post_process_func,
                                                                 date_column=self.charges_date_column,
                                                                 numeric_columns=self.charges_numeric_columns,
                                                                 start_date=start_date,
                                                                 end_date=end_date,
                                                                 dry_run=dry_run)

        self.reconciled_df = reconcile_charges_and_ledger(self.tradeledger_df,
                                                     self.charges_aggregate_df,
                                                     ledger_date_column=self.fledger_date_column,
                                                     charges_date_column=self.charges_date_column)

        print('Missing Entries')
        self.unmatched_df = find_unmatched(self.reconciled_df,
                                      ledger_date_column=self.fledger_date_column,
                                      charges_date_column=self.charges_date_column)

        generate_report_from_unmatched(self.unmatched_df,
                                       left_on=self.fledger_date_column,
                                       right_on=self.charges_date_column,
                                       left_report=financialledger_document_name,
                                       right_report=charges_document_name)