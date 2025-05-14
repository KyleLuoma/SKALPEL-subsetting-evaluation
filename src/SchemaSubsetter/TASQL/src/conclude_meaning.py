import os
import json
from SchemaSubsetter.TASQL.src.llm import collect_response
from SchemaSubsetter.TASQL.src.prompt_bank import column_meaning_prompt
from SchemaSubsetter.TASQL.src.utils import new_directory
import csv
import sqlite3
import tqdm
import argparse


# Skalpel imports:
from NlSqlBenchmark.NlSqlBenchmark import NlSqlBenchmark
import time

def get_prompts(db_root_path, table_json):
    prompt_dic = {}
    for i in tqdm.tqdm(range(len(table_json))):
        table_info = table_json[i]
        db_id = table_info['db_id']
        db_path = os.path.join(db_root_path, db_id, f'{db_id}.sqlite')
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        csv_dir = os.path.join(db_root_path,db_id,'database_description')
        otn_list = table_info['table_names_original']
        tn_list = table_info['table_names']
        for i, otn in enumerate(otn_list):
            table_name = tn_list[i]
            csv_path = os.path.join(os.path.join(csv_dir, f"{otn}.csv"))
            csv_dict = csv.DictReader(open(csv_path, newline='', encoding="latin1"))
            
            for row in csv_dict:
                headers = list(row.keys())
                ocn_header = [h for h in headers if 'original_column_name' in h][0]  # remove BOM
                ocn, cn = row[ocn_header].strip(), row['column_name']
                column_description = row['column_description'].strip()
                column_type = row['data_format'].strip()
                column_name = cn if cn not in ['', ' '] else ocn
                value_description = row['value_description'].strip()
                column_description = None if (column_description in ['',' '] or column_description == ocn or column_description == column_name) else column_description
                value_description = None if (value_description in ['',' '] or value_description == ocn or value_description == column_name) else value_description
                column_type = None if column_type in ['', ' '] else column_type
                
                if column_type == None and column_description == None:
                    continue
                
                input_paras = f"database_id = '{db_id}', table_name = '{table_name}', column_name = '{column_name}', column_type = '{column_type}'"
                if column_description:
                    input_paras += f", column_description = '{column_description}'"
                if value_description:
                    input_paras += f", value_description = '{value_description}'"
                if column_type in ['text', 'date', 'datetime']:
                    sql = f'''SELECT DISTINCT "{ocn}" FROM `{otn}` where "{ocn}" IS NOT NULL ORDER BY RANDOM()'''
                    cursor.execute(sql)
                    values = cursor.fetchall()
                    if len(values) > 0 and len(values[0][0]) < 50:
                        if len(values) <= 10:
                            all_possible_values = [v[0] for v in values]
                            example_values = None
                        else:
                            all_possible_values = None
                            example_values = [v[0] for v in values[:3]]
                            
                    if example_values:
                        input_paras += f', example_values = {example_values}'
                    if all_possible_values:
                        input_paras += f', all_possible_values = {all_possible_values}'
                
                prompt_dic[f'{db_id}|{otn}|{ocn}'] = column_meaning_prompt.format(input_paras = input_paras)
    return prompt_dic

# Skalpel mod: use skalpel benchmark format instead of Bird directory / file structure 
def get_prompts_skalpel(benchmark: NlSqlBenchmark):
    table_json = []
    for db in benchmark.databases:
        table_json.append(benchmark.get_active_schema().as_bird_json_format())
    prompt_dic = {}
    processing_times = {}
    start_schema = benchmark.get_active_schema().database
    for db in benchmark.databases:
        s_time = time.perf_counter()
        benchmark.set_active_schema(db)
        schema = benchmark.get_active_schema()
        for table in schema.tables:
            for column in table.columns:
                input_paras = f"database_id = '{schema.database}', table_name = '{table.name}', column_name = '{column.name}', column_type = '{column.data_type}'"
                if column.description != None and column.description != "":
                    input_paras += f", column_description = '{column.description}'"
                if column.value_description != None and column.value_description != "":
                    input_paras += f", value_description = '{column.value_description}'"
                if column.data_type in ['text', 'date', 'datetime']:
                    values = benchmark.get_sample_values(
                        table_name=table.name, column_name=column.name, num_values=50
                    )
                    if len(values) > 0 and len(values) <= 10:
                        all_possible_values = [v[0] for v in values]
                        example_values = None
                    else:
                        all_possible_values = None
                        example_values = [v for v in values[:3]]
                    if example_values:
                        input_paras += f', example_values = {example_values}'
                    if all_possible_values:
                        input_paras += f', all_possible_values = {all_possible_values}'
                prompt_dic[f'{db}|{table.name}|{column.name}'] = column_meaning_prompt.format(input_paras = input_paras)
        e_time = time.perf_counter()
        processing_times[db] = e_time - s_time
    benchmark.set_active_schema(start_schema)
    return prompt_dic, processing_times

# Skalpel mod: added performance timing for each db, and skip already processed boolean
def conclude_each_column(prompt_dic, output_path, skip_already_processed: bool = True):
    output_dic = {}
    processing_times = {}

    for column, prompt in tqdm.tqdm(prompt_dic.items(), desc="Generating column descriptions"):
        s_time = time.perf_counter()
        db = column.split("|")[0]
        if db not in processing_times.keys():
            processing_times[db] = 0

        output = collect_response(prompt, max_tokens = 800, stop = '\n')
        output_dic[column] = output
        
        if os.path.exists(output_path) and skip_already_processed:
            with open(output_path, 'r') as f:
                contents = json.loads(f.read())
        else:
            with open(output_path, 'a') as f:
                contents = {}
        contents.update(output_dic)
        json.dump(output_dic, open(output_path, 'w'), indent=4)
        e_time = time.perf_counter()
        processing_times[db] += e_time - s_time
    return processing_times


def parser():
    parser = argparse.ArgumentParser("")
    parser.add_argument('--db_root_path', type=str, default="./data/dev_databases")
    parser.add_argument('--mode', type=str, default='dev')
    parser.add_argument('--output_path', type=str, default="./outputs/column_meaning.json")
    opt = parser.parse_args()
    return opt

def main(opt):
    db_root_path = opt.db_root_path
    output_path = opt.output_path
    mode = opt.mode
    
    table_json_path = os.path.join(db_root_path, f'{mode}_tables.json')
    table_json = json.load(open(table_json_path, 'r'))
    prompt_dic = get_prompts(db_root_path, table_json)
    conclude_each_column(prompt_dic, output_path)

if __name__ == '__main__':
    opt = parser()
    main(opt)
