import pandas as pd

def clean_df(df):
    """
        Clean pd dataframe. replace NaN with 0.
        collapse columns into joint columns by summing key total
    """
    output = df.fillna(0)
    output['Total'] = df.sum(axis=1)
    output = output[['Total']]
    # output = output.rename(columns={output.columns[0]: "str_key" })
    # output = output.set_index(['str_key'])
    return output