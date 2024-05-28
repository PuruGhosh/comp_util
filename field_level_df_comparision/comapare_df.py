import datetime

from pandas import DataFrame as df
import pandas as pd

data1 = {'Name': ['John', 'Anna', 'Peter', 'Linda'],
         'Age': [25, 32, 41, 29],
         'City': ['New York', 'Paris', 'London', 'Berlin'],
         'Roll': [1, 2, 3, 4]
         }

data2 = {'Name': ['John', 'Anna', 'Robin', 'Linda', 'Sam'],
         'Age': [25, 32, 44, 29, 33],
         'City': ['New York', 'Hamburg', 'Dublin', 'Berlin', 'Hongkong'],
         'Roll': [1, 2, 5, 4, 4]
         }
src_df = pd.DataFrame(data1)
trg_df = pd.DataFrame(data2)


def check_duplicate_id(src_df: df, trg_df: df, src_id_field="_id", trg_id_field="_id"):
    if trg_id_field == "_id" and src_id_field != "_id":
        trg_id_field = src_id_field

    src_repeating_ids = src_df[src_df.duplicated(src_id_field)][src_id_field].tolist()

    # Find repeating IDs in target DataFrame
    trg_repeating_ids = trg_df[trg_df.duplicated(trg_id_field)][trg_id_field].tolist()

    # Return dictionary containing repeating IDs
    return {
        "src_df_repeating_ids": src_repeating_ids,
        "trg_df_repeating_ids": trg_repeating_ids
    }


def get_common_fields(src_df: df, trg_df: df):
    common_field_names = set(src_df.columns) & set(trg_df.columns)
    return list(common_field_names)


def compare_df(src_df: df, trg_df: df, id_name: str, result_dir: str, file_name_prefix="report"):
    mis_match_id_column_map = check_duplicate_id(src_df, trg_df, "Roll")
    if mis_match_id_column_map['src_df_repeating_ids']:
        print("duplicate id found in src df\n",mis_match_id_column_map['src_df_repeating_ids'])
    if mis_match_id_column_map['trg_df_repeating_ids']:
        print("duplicate id found in trg df\n",mis_match_id_column_map['trg_df_repeating_ids'])
    # print(mis_match_id_column_map)
    merged_df = df.merge(src_df, trg_df, on=id_name, how="outer", suffixes=("_src", "_trg"), indicator=True)
    print(merged_df.to_string(index=False))

    left_only_df = merged_df[merged_df['_merge'] == 'left_only'].rename(columns=lambda x: x.replace('_src', '')).filter(
        regex=r'^(?!.*_trg$)')

    # Create DataFrame for rows with _merge='right_only' and remove suffix '_trg'
    right_only_df = merged_df[merged_df['_merge'] == 'right_only'].rename(
        columns=lambda x: x.replace('_trg', '')).filter(regex=r'^(?!.*_src$)')

    src_columns = merged_df.filter(like='_src').columns
    unequal_df = pd.DataFrame(columns=merged_df.columns)
    equal_df = pd.DataFrame(columns=merged_df.columns)
    mismatch_map = {}
    for index, row in merged_df[merged_df['_merge'] == 'both'].iterrows():
        ls = []
        for src_col in src_columns:
            trg_col = src_col.replace('_src', '_trg')
            if not row[src_col] == row[trg_col]:
                ls.append(src_col.replace('_src', ''))
        if ls:
            mismatch_map[row[id_name]] = ls
            unequal_df = pd.concat([unequal_df, pd.DataFrame([row])], ignore_index=True)
        else:
            equal_df = pd.concat([equal_df, pd.DataFrame([row])], ignore_index=True)
    print("Mismatch dict")
    print(mismatch_map)
    print("left only")
    print(left_only_df)
    print("right_only_df")
    print(right_only_df)
    print("both_match_df")
    print(equal_df)

    current_time_stamp = datetime.datetime.now().strftime("%Y-%m-%dT%H-%M-%S")
    file_name = f'{result_dir}/{file_name_prefix}_{current_time_stamp}.xlsx'
    summary_map = {
        "src_total_count":[src_df.shape[0]],
        "trg_total_count":[trg_df.shape[0]],
        "both_matched": [equal_df.shape[0]],
        "only_in_src":[left_only_df.shape[0]],
        "only_in_trg":[right_only_df.shape[0]],
        "mismatched":[unequal_df.shape[0]]
    }
    summary_df = pd.DataFrame(summary_map)
    with pd.ExcelWriter(file_name) as writer:
        right_only_df.to_excel(writer, sheet_name="right_only", index=False)
        left_only_df.to_excel(writer, sheet_name="left_only", index=False)
        equal_df.to_excel(writer, sheet_name="equal_both", index=False)
        unequal_df.to_excel(writer, sheet_name="mismatched", index=False)
        summary_df.to_excel(writer,sheet_name='summary',index=False)

compare_df(src_df, trg_df, 'Roll', './result')
