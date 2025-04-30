import json
from tqdm import tqdm
from SchemaSubsetter.RSLSQL.src.utils.db_op import get_table_infos, get_foreign_key_infos, get_throw_row_data
import os
import time
from SchemaSubsetter.RSLSQL.src.configs.config import dev_json_path


def load_json(file_path):
    with open(file_path, 'r') as f:
        return json.load(f)


def save_json(data, file_path):
    with open(file_path, 'w') as f:
        json.dump(data, f, indent=4, ensure_ascii=False)


def generate_db_list(ppls):
    db_list = []
    for ppl in tqdm(ppls):
        db_name = ppl['db_id']
        if db_name not in db_list:
            db_list.append(db_name)
    return db_list


def generate_table_infos(db_list):
    table_str_list = []
    for table in tqdm(db_list):
        table_str = get_table_infos(table)
        table_str_list.append(table_str)
        print(table_str)
        print('-------------------' * 10)
    return table_str_list



def create_output_structure(ppls, table_infos):
    outputs = []
    # Skalpel mod: return dict of processing times by database
    processing_times = {}
    timing_db = ""
    # Skalpel mod: cache ddl data for each db to avoid reprocessing
    # for every question
    ddl_cache = {}

    for i in tqdm(range(len(ppls)), desc="Creating output structure."):
        ppl = ppls[i]
        evidence = ppl['evidence']
        db_name = ppl['db_id']

        # Skalpel mod: return dict of processing times by database
        if db_name not in processing_times.keys():
            processing_times[db_name] = time.perf_counter()
            if timing_db != "":
                processing_times[timing_db] = time.perf_counter() - processing_times[timing_db]
            timing_db = db_name

        question = ppl['question']
        # SQL = ppl['SQL']
        foreign_str = get_foreign_key_infos(db_name)

        table_str = next((info['simplified_ddl'] for info in table_infos if info['db'] == db_name), None)
        if db_name not in ddl_cache.keys():
            ddl_data = "#\n" + get_throw_row_data(db_name).strip() + "\n# "
            ddl_cache[db_name] = ddl_data
        else:
            ddl_data = ddl_cache[db_name]

        output = {
            'db': db_name,
            'question': question,
            'simplified_ddl': table_str,
            'ddl_data': ddl_data,
            'evidence': evidence,
            'foreign_key': foreign_str
        }
        outputs.append(output)
    processing_times[timing_db] = time.perf_counter() - processing_times[timing_db]
    return outputs, processing_times


def generate_ppl_dev_json(dev_file, out_file):
    ppls = load_json(dev_file)
    db_list = generate_db_list(ppls)
    table_str_list = generate_table_infos(db_list)

    all_table_info = [{'db': db_list[i], 'simplified_ddl': table_str_list[i]} for i in range(len(db_list))]
    save_json(all_table_info, 'src/SchemaSubsetter/RSLSQL/src/information/describle.json')

    table_infos = load_json('src/SchemaSubsetter/RSLSQL/src/information/describle.json')
    outputs, processing_times = create_output_structure(ppls, table_infos)

    save_json(outputs, out_file)

    os.remove('src/SchemaSubsetter/RSLSQL/src/information/describle.json')

    return processing_times

#Skalpel mod, prevent exec on import
if __name__ == "__main__":
    generate_ppl_dev_json(dev_json_path, 'src/information/ppl_dev.json')
