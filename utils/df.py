import pandas as pd


def df_print(df, dtypes=False, index=False, shape=False, new_line=True, gui=False, active=True, location=True):
    if not active:
        return

    # https://stackoverflow.com/questions/6810999/how-to-determine-file-function-and-line-number

    pd.set_option('display.max_rows', None)
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', None)
    pd.set_option('display.max_colwidth', None)
    pd.set_option('display.float_format', lambda x: '%.2f' % x)

    if gui:
        # gui = show(df, settings={'block': True})
        print("pandas_gui not used")
    else:
        if new_line:
            print()

        print(df)

        if index:
            print(df.index)

        if shape:
            print(df.shape)

        if dtypes:
            print(df.dtypes)
