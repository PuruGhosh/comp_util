import datetime

import pandas as pd
from pandas import DataFrame as df
from tabulate import tabulate as tb

data1 = {'Name': ['John', 'Anna', 'Peter', 'Linda'], 'Age': [25, 32, 41, 29],
         'City': ['New York', 'Paris', 'London', 'Berlin'], 'Roll': [1, 2, 3, 4]}

data2 = {'Name': ['John', 'Anna', 'Robin', 'Linda', 'Sam'], 'Age': [25, 32, 44, 29, 33],
         'City': ['New York', 'Hamburg', 'Dublin', 'Berlin', 'Hongkong'], 'Roll': [1, 2, 5, 4, 4]}
test_src_df = pd.DataFrame(data1)
test_trg_df = pd.DataFrame(data2)


def check_duplicate_id(src_df: df, trg_df: df, src_id_field="_id", trg_id_field="_id"):
    if trg_id_field == "_id" and src_id_field != "_id":
        trg_id_field = src_id_field

    src_repeating_ids = src_df[src_df.duplicated(src_id_field)][src_id_field].tolist()

    # Find repeating IDs in target DataFrame
    trg_repeating_ids = trg_df[trg_df.duplicated(trg_id_field)][trg_id_field].tolist()

    # Return dictionary containing repeating IDs
    return {"src_df_repeating_ids": src_repeating_ids, "trg_df_repeating_ids": trg_repeating_ids}


def get_common_fields(src_df: df, trg_df: df):
    common_field_names = set(src_df.columns) & set(trg_df.columns)
    return list(common_field_names)


def compare_df(src_df: df, trg_df: df, id_name: str, result_dir: str, file_name_prefix="report"):
    # Checking for duplicate ids
    mis_match_id_column_map = check_duplicate_id(src_df, trg_df, "Roll")
    if mis_match_id_column_map['src_df_repeating_ids']:
        print(f"duplicate {id_name} found in src df\n", mis_match_id_column_map['src_df_repeating_ids'])
        print(f"Removing source df repeating {id_name}")
        src_df = src_df.drop_duplicates(id_name, keep='last')
    if mis_match_id_column_map['trg_df_repeating_ids']:
        print(f"duplicate {id_name} found in trg df\n", mis_match_id_column_map['trg_df_repeating_ids'])
        print(f"Removing target df repeating {id_name}")
        trg_df = trg_df.drop_duplicates(id_name, keep='last')

    # Checking for common fields
    common_fields = get_common_fields(src_df, trg_df)
    if not common_fields:
        raise ValueError(f"No common fields found in src df fields {src_df.columns} and trg df fields {trg_df.columns}")
    if id_name not in common_fields:
        raise ValueError(f"{id_name} is not common to src df{src_df.columns} and trg df {trg_df.columns}.")
    print(f"Comparing on fields {common_fields} wrt {id_name}.")
    src_df = src_df[common_fields]
    trg_df = trg_df[common_fields]

    for field in common_fields:
        if src_df[field].dtype != trg_df[field].dtype:
            print(f"""data type mismatch for field {field}, src d_type {src_df[field].dtype}, 
                trg d_type {trg_df[field].dtype}. Casting both as string""")
            src_df[field] = src_df[field].astype(str)
            trg_df[field] = trg_df[field].astype(str)
    # Comparing starts
    merged_df = df.merge(src_df, trg_df, on=id_name, how="outer", suffixes=("_src", "_trg"), indicator=True)
    print(f"Total src df count {src_df.shape[0]}, trg df count: {trg_df.shape[0]}")
    print(tb(merged_df.head(50), headers='keys', tablefmt='pretty'))

    # Contains data only in src df
    left_only_df = merged_df[merged_df['_merge'] == 'left_only'].rename(columns=lambda x: x.replace('_src', '')).filter(
        regex=r'^(?!.*_trg$)')

    # Contains data only in trg df
    right_only_df = merged_df[merged_df['_merge'] == 'right_only'].rename(
        columns=lambda x: x.replace('_trg', '')).filter(regex=r'^(?!.*_src$)')

    # Checking for mismatch
    src_columns = merged_df.filter(like='_src').columns
    unequal_df = merged_df.iloc[:0, :].copy()
    equal_df = merged_df.iloc[:0, :].copy()
    mismatch_map = {}
    for index, row in merged_df[merged_df['_merge'] == 'both'].iterrows():
        ls = []
        for src_col in src_columns:
            trg_col = src_col.replace('_src', '_trg')
            if not row[src_col] == row[trg_col]:
                ls.append(src_col.replace('_src', ''))
        if ls:
            mismatch_map[row[id_name]] = [', '.join(str(item) for item in ls)]
            unequal_df = pd.concat([unequal_df, pd.DataFrame([row])], ignore_index=True)
        else:
            equal_df = pd.concat([equal_df, pd.DataFrame([row])], ignore_index=True)

    summary_map = {"src_total_count": [src_df.shape[0]], "trg_total_count": [trg_df.shape[0]],
                   "both_matched": [equal_df.shape[0]], "only_in_src": [left_only_df.shape[0]],
                   "only_in_trg": [right_only_df.shape[0]], "mismatched": [unequal_df.shape[0]]}
    summary_df = pd.DataFrame(summary_map)
    mismatch_df = pd.DataFrame(list(mismatch_map.items()), columns=[id_name, 'fields_mismatched'])

    print(f"left only. Count: {left_only_df.shape[0]}")
    print(tb(left_only_df, headers='keys', tablefmt='pretty'))
    print(f"right_only_df. Count: {right_only_df.shape[0]}")
    print(tb(right_only_df, headers='keys', tablefmt='pretty'))
    print(f"both_match_df. Count: {equal_df.shape[0]}")
    print(tb(equal_df, headers='keys', tablefmt='pretty'))
    print(f"Mismatch df. Count: {mismatch_df.shape[0]}")
    print(tb(mismatch_df, headers='keys', tablefmt='pretty'))
    print("Summary")
    print(tb(summary_df, headers='keys', tablefmt='pretty'))

    current_time_stamp = datetime.datetime.now().strftime("%Y-%m-%dT%H-%M-%S")
    file_name = f'{result_dir}/{file_name_prefix}_{current_time_stamp}.xlsx'
    with pd.ExcelWriter(file_name) as writer:
        right_only_df.to_excel(writer, sheet_name="right_only", index=False)
        left_only_df.to_excel(writer, sheet_name="left_only", index=False)
        equal_df.to_excel(writer, sheet_name="equal_both", index=False)
        unequal_df.to_excel(writer, sheet_name="mismatched", index=False)
        summary_df.to_excel(writer, sheet_name='summary', index=False)
        mismatch_df.to_excel(writer, sheet_name='mismatched_fields', index=False)


compare_df(test_src_df, test_trg_df, 'Roll', './result')
