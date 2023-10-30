import decimal
import os
import camelot
from pypdf import PdfReader
import pandas as pd
import numpy as np
from decimal import Decimal, InvalidOperation
import re
from datetime import datetime
from utils.debug import df_print, debug_log


whitespace_regex = r"^\s*$"
bracketed_number_regex = r"^\(([\d.,]*)\)"
date_regex_list = [
    # Zerodha
    (r"(\d{4}-\d{2}-\d{2})","%Y-%m-%d"),
    # Axisdirect
    (r"(\d{8})\.","%d%m%Y"),
]


def convert_datestr_to_isostr(datestr, format):
    if not isinstance(datestr, str):
        return datestr

    date = datetime.strptime(datestr, format)
    return date.strftime("%Y-%m-%d")


def get_date_from_string(input_str):
    iso_str = None
    for (date_regex, date_format) in date_regex_list:
        match = re.search(date_regex, input_str)
        if match is not None:
            date_str = match.group(1)
            iso_str = convert_datestr_to_isostr(date_str, date_format)
            break

    return iso_str


def get_dataframe_from_camelot_table(table):
    header_row = table.df.iloc[0]
    header_row = header_row.map(lambda cell: cell.replace("\n", " "))
    df_with_column_headers = pd.DataFrame(table.df.values[1:], columns=header_row)
    # df_print(df_with_column_headers)
    return df_with_column_headers


def convert_to_decimal_or_blank(cell):
    if isinstance(cell, Decimal):
        debug_log(f"cell value '{cell}' is already a Decimal", active=False)
        return cell

    input_value = cell
    if not isinstance(cell, str):
        input_value = str(cell)

    value = np.NaN
    try:
        whitespace_match = re.match(whitespace_regex, input_value)
        if whitespace_match is not None:
            value = 0
        else:
            match = re.match(bracketed_number_regex, input_value)

            try:
                if match is not None:
                    value = -Decimal(match.group(1))
                else:
                    value = Decimal(input_value)
            except InvalidOperation as e:
                print(type(e).__name__, f"Error! cant convert {cell} to decimal")
    except TypeError as e:
        debug_log(f"Error! matching cell '{cell}'[{type(cell)}] with whitespace_regex '{whitespace_regex}'")

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


def get_pdf_number_of_pages(pdf_file_path):
    reader = PdfReader(pdf_file_path)
    return len(reader.pages)


def get_charges_aggregate_df_from_pdf(pdf_file_path, date, *,
                                      num_last_pages=0,
                                      numeric_columns=None,
                                      summary_match_func=None,
                                      summary_post_process_func=None
                                      ):
    if pdf_file_path is None:
        raise RuntimeError(f"pdf_file_path is not provided")

    if numeric_columns is None:
        raise RuntimeError(f"numeric_columns is not provided")

    if summary_match_func is None:
        raise RuntimeError(f"summary_match_func is not provided")

    if summary_post_process_func is None:
        raise RuntimeError(f"summary_post_process_func is not provided")

    # df_print(pdf_file_path)

    document_num_pages = get_pdf_number_of_pages(pdf_file_path)
    # df_print(f"{pdf_file_path}: Number of pages:", document_num_pages)

    if num_last_pages > 0:
        page_list = []
        for i in range(num_last_pages):
            page_num = document_num_pages - i
            if page_num > 0:
                page_list.append(str(page_num))
        last_pages_str = ",".join(page_list)
    else:
        last_pages_str = "all"

    debug_log("last_pages_str:", last_pages_str)

    try:
        tables = camelot.read_pdf(pdf_file_path, pages=last_pages_str)

        print(f"{pdf_file_path}:  {len(tables)} Tables detected on pages:{last_pages_str} ")

        summary_df = None
        for table in tables:
            df = get_dataframe_from_camelot_table(table)
            doc_info = summary_match_func(df, page_num=table.page)
            if doc_info and doc_info['tag'] == 'Summary':
                summary_df = df
                break

        if summary_df is None:
            raise RuntimeError(f"Summary table not found in file '{pdf_file_path}'")

        if summary_post_process_func is not None:
            charges_sum_df = summary_post_process_func(pdf_file_path, date, summary_df)

            df_print(charges_sum_df, active=False)

            return charges_sum_df
    except ValueError as e:
        debug_log(f"Error! {type(e)} reading file '{pdf_file_path}'")

    return pd.DataFrame()


debug_process = True


def process_contractnotes_folder(cnotes_folder_path, *,
                                 num_last_pages=0,
                                 charges_aggregate_file_path=None,
                                 summary_match_func=None,
                                 summary_post_process_func=None,
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

            # We could have a custom function here
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

            charges_sum_df = get_charges_aggregate_df_from_pdf(pdf_file_path,
                                                               date,
                                                               num_last_pages=num_last_pages,
                                                               numeric_columns=numeric_columns,
                                                               summary_match_func=summary_match_func,
                                                               summary_post_process_func=summary_post_process_func
                                                               )
            if not charges_sum_df.empty:
                aggregate_df = pd.concat([aggregate_df, charges_sum_df], axis=0)
                count += 1

    if aggregate_df is not None:
        df_print(aggregate_df, active=True)  # Show

    if count > 0:
        # We convert the decimal columns to float
        # aggregate_df = aggregate_df.map(float)
        create_output_file(aggregate_df, charges_aggregate_file_path, dry_run=dry_run)

    return aggregate_df


def process_financialledger_file(data_file, *, date_column='Date', date_format=None, post_process_func=None, start_date=None, end_date=None, max_count=0):
    fledger_df = pd.read_excel(data_file)

    if date_format is not None:
        fledger_df[date_column] = fledger_df[date_column].map(lambda x: convert_datestr_to_isostr(x, date_format) )

    if post_process_func is not None:
        fledger_df = post_process_func(fledger_df)

    df_print(fledger_df, active=True, shape=True)

    return fledger_df


def reconcile_charges_and_ledger(ledger_df, charges_df, *, ledger_date_column='Date', charges_date_column='Date'):
    # df_print(ledger_df)
    # df_print(charges_df)

    # We will do an outer join
    merged_df = ledger_df.merge(charges_df, left_on=ledger_date_column, right_on=charges_date_column, how='outer')
    # df_print(merged_df)
    return merged_df


def find_missing_entries(joined_df, *, ledger_date_column='Date', charges_date_column='Date'):
    mismatch_df = joined_df[joined_df[ledger_date_column] != joined_df[charges_date_column]]
    # df_print(mismatch_df)
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
                 fledger_date_format=None,
                 fledger_post_process_func=None,
                 cnote_num_last_pages=2,
                 charges_date_column='Date',
                 charges_numeric_columns=None,
                 summary_match_func=None,
                 summary_post_process_func=None):
        super(Broker, self).__init__(name, "Broker")
        self.fledger_path = os.path.join(input_path_prefix, f'FinancialLedger/{self.name}/{self.name}_FinancialLedger_Transactions.xlsx')
        self.cnote_folder_path = os.path.join(input_path_prefix, f'ContractNotes/{self.name}')
        self.charges_file_path = os.path.join(compute_path_prefix, self.name, f'charges.{self.output_format}')
        self.summary_match_func = summary_match_func
        self.summary_post_process_func = summary_post_process_func
        self.fledger_post_process_func = fledger_post_process_func
        self.fledger_date_column = fledger_date_column
        self.fledger_date_format = fledger_date_format
        self.cnote_num_last_pages = cnote_num_last_pages
        self.charges_date_column = charges_date_column
        self.charges_numeric_columns = charges_numeric_columns

        self.tradeledger_df = None
        self.summary_aggregate_df = None
        self.reconciled_df = None
        self.missing_missing_df = None

    def read_ledger(self, start_date=None, end_date=None):
        self.tradeledger_df = process_financialledger_file(self.fledger_path,
                                                           post_process_func=self.fledger_post_process_func,
                                                           date_column=self.fledger_date_column,
                                                           date_format=self.fledger_date_format,
                                                           start_date=start_date,
                                                           end_date=end_date)
        df_print(self.tradeledger_df, active=False)

    def read_contract_notes(self, start_date=None, end_date=None, dry_run=False, max_count=0):
        self.summary_aggregate_df = process_contractnotes_folder(self.cnote_folder_path,
                                                                 charges_aggregate_file_path=self.charges_file_path,
                                                                 num_last_pages=self.cnote_num_last_pages,
                                                                 summary_match_func=self.summary_match_func,
                                                                 summary_post_process_func=self.summary_post_process_func,
                                                                 date_column=self.charges_date_column,
                                                                 numeric_columns=self.charges_numeric_columns,
                                                                 start_date=start_date,
                                                                 end_date=end_date,
                                                                 dry_run=dry_run,
                                                                 max_count=max_count)

    def reconcile(self, start_date=None, end_date=None):
        self.reconciled_df = reconcile_charges_and_ledger(self.tradeledger_df,
                                                          self.summary_aggregate_df,
                                                          ledger_date_column=self.fledger_date_column,
                                                          charges_date_column=self.charges_date_column)

        self.missing_missing_df = find_missing_entries(self.reconciled_df,
                                                       ledger_date_column=self.fledger_date_column,
                                                       charges_date_column=self.charges_date_column)

        if len(self.missing_missing_df) == 0:
            debug_log(f"Now we should look for mismatch entries")

    def report(self):
        if len(self.missing_missing_df):
            print('Missing Entries')
            charges_document_name = f'{self.name} Charges Aggregate'
            financialledger_document_name = f'{self.name} Financial Ledger'

            generate_report_from_unmatched(self.missing_missing_df,
                                           left_on=self.fledger_date_column,
                                           right_on=self.charges_date_column,
                                           left_report=financialledger_document_name,
                                           right_report=charges_document_name)
        else:
            debug_log(f"There are no missing entries!", location=False)

    def compute(self, start_date=None, end_date=None, dry_run=False, max_count=0):
        self.read_ledger(start_date=start_date, end_date=end_date)
        self.read_contract_notes(start_date=start_date, end_date=end_date, dry_run=dry_run, max_count=max_count)
        self.reconcile(start_date=start_date, end_date=end_date)
        self.report()



