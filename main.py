from broker import *

pd_set_options()

data_type = 'sample'
account_provider_name = 'Zerodha'

output_folder = f'output/{data_type}/{account_provider_name}'
output_format = 'xlsx'

financialledger_file_path = f'data/{data_type}/FinancialLedger/Zerodha/Zerodha_FinancialLedger_Transactions.xlsx'
financialledger_date_column = 'Posting Date'
contractnotes_folder = f'data/{data_type}/ContractNotes/Zerodha'

start_date = "2022-04-01"
if data_type == 'sample':
    end_date = "2023-01-31"
else:
    end_date = "2023-04-01"


zerodha_broker = Broker('Zerodha',
                        input_path_prefix=f'data/{data_type}',
                        output_path_prefix=f'data/{data_type}',
                        fledger_date_column='Posting Date',
                        charges_date_column='Date',
                        charges_numeric_columns=['Equity', 'Equity (T+1)', 'Futures and Options', 'NET TOTAL'],
                        )
zerodha_broker.compute(start_date=start_date, end_date=end_date)
# exit(0)

charges_file_name = f'charges.{output_format}'
charges_file_path = os.path.join(output_folder, charges_file_name)
charges_date_column = 'Date'


charges_document_name = f'{account_provider_name} Charges Aggregate'
financialledger_document_name = f'{account_provider_name} Financial Ledger'
zerodha_numeric_columns = ['Equity', 'Equity (T+1)', 'Futures and Options', 'NET TOTAL']



tradeledger_df = process_financialledger_file(financialledger_file_path,
                                              start_date=start_date,
                                              end_date=end_date)

# if not os.path.exists(charges_file_path):
charges_aggregate_df = process_contractnotes_folder(contractnotes_folder,
                                                    aggregate_file_path=charges_file_path,
                                                    date_column=charges_date_column,
                                                    numeric_columns=zerodha_numeric_columns,
                                                    start_date=start_date,
                                                    end_date=end_date,
                                                    dry_run=True)


reconciled_df = reconcile_charges_and_ledger(tradeledger_df,
                                             charges_aggregate_df,
                                             ledger_date_column=financialledger_date_column,
                                             charges_date_column=charges_date_column)

print('Missing Entries')
unmatched_df = find_unmatched(reconciled_df,
                              ledger_date_column=financialledger_date_column,
                              charges_date_column=charges_date_column)

generate_report_from_unmatched(unmatched_df,
                               left_on=financialledger_date_column,
                               right_on=charges_date_column,
                               left_report=financialledger_document_name,
                               right_report=charges_document_name)

