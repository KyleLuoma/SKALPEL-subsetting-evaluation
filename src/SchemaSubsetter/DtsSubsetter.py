from SchemaSubsetter.SchemaSubsetter import SchemaSubsetter
from SchemaSubsetter.SchemaSubsetterResult import SchemaSubsetterResult
from NlSqlBenchmark.NlSqlBenchmark import NlSqlBenchmark
from NlSqlBenchmark.SchemaObjects import (
    Schema,
    SchemaTable,
    TableColumn,
    ForeignKey
)
from NlSqlBenchmark.BenchmarkQuestion import BenchmarkQuestion
from tqdm import tqdm
from importlib import import_module

import torch
import re
import os
from transformers import AutoTokenizer, BitsAndBytesConfig
from transformers import AutoModelForCausalLM
from torch import cuda
from sql_metadata import Parser
from tqdm import tqdm
from transformers import StoppingCriteria

class DtsSubsetter(SchemaSubsetter):

    name = "dtssql"
    uses_gpu = True

    def __init__(self, benchmark: NlSqlBenchmark = None, device: int = None):
        self.benchmark = benchmark
        os.environ["CUDA_VISIBLE_DEVICES"]="0,1,2"
        device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        model_name = "deepseek-ai/deepseek-coder-6.7b-instruct"
        model_name = "MrezaPRZ/DeepSchema_BIRD"
        # model_name = "mistralai/Mistral-7B-Instruct-v0.2"
        bnb_config = BitsAndBytesConfig(
            load_in_8bit=True,
            bnb_4bit_compute_dtype = torch.float16,
            bnb_4bit_quant_type='nf4',
            bnb_4bit_use_double_quant=True,
        )
        self.model = AutoModelForCausalLM.from_pretrained(
            model_name,
            attn_implementation="flash_attention_2", # use with amper architecture
            torch_dtype=torch.bfloat16,
            #quantization_config=bnb_config, # use when low on memory
            device_map = "auto"
            # device_map = {f'cuda:{i}': i - 4 for i in range(4, 8)}
        )

        self.tokenizer = AutoTokenizer.from_pretrained(model_name)
        self.tokenizer.pad_token = self.tokenizer.eos_token
        self.tokenizer.pad_token_id = self.tokenizer.eos_token_id
        self.tokenizer.encode(' ;')
        self.schema_string_cache = {}

        
    class EosListStoppingCriteria(StoppingCriteria):
        def __init__(self, eos_sequence = [6203]):
            self.eos_sequence = eos_sequence

        def __call__(self, input_ids: torch.LongTensor, scores: torch.FloatTensor, **kwargs) -> bool:
            last_ids = input_ids[:,-len(self.eos_sequence):].tolist()
            return self.eos_sequence in last_ids
        
    def append_string_to_file(self, text, file_path):
        with open(file_path, 'a') as file:
            file.write(text + '\n')

    def remove_spaces(self, text):
        return re.sub(r'\s+', ' ', text)

    def call_mistral(self, inputs):
        if len(inputs[0]) > 4096 - 250:
            raise Exception("Inputs greater than context window")
            
        output_tokens = self.model.generate(
            inputs, 
            max_new_tokens=250, 
            do_sample=False, 
            pad_token_id=self.tokenizer.eos_token_id, 
            eos_token_id=self.tokenizer.eos_token_id, 
            stopping_criteria = [DtsSubsetter.EosListStoppingCriteria()])
        total_tokens = len(inputs[0]) + len(output_tokens[0])
        return self.tokenizer.decode(output_tokens[0][len(inputs[0]):], skip_special_tokens=True), total_tokens


    def _make_schema_string(self, schema: Schema) -> str:
        schema_string = ""
        for table in schema.tables:
            schema_string += table.as_ddl()
            sample_values = {}
            for column in table.columns:
                sample_values[column.name] = self.benchmark.get_sample_values(
                    table_name=table.name,
                    column_name=column.name,
                    num_values=3
                )
            schema_string += f"Sample rows from `{table.name}`:"
            for i in range(0, min(3, len(sample_values[table.columns[0].name]))):
                try:
                    schema_string += f"\n{', '.join([str(sample_values[k][i]) for k in sample_values.keys()])}"
                except IndexError:
                    pass
            schema_string += "\n\n"
        return schema_string


    def get_schema_subset(
            self,
            benchmark_question: BenchmarkQuestion
            ) -> SchemaSubsetterResult:
        question = benchmark_question.question 
        query = benchmark_question.query
        if benchmark_question.schema.database not in self.schema_string_cache.keys():
            database_schema = self._make_schema_string(benchmark_question.schema)
            self.schema_string_cache[benchmark_question.schema.database] = database_schema
        else:
            database_schema = self.schema_string_cache[benchmark_question.schema.database]
        db_id = benchmark_question.schema.database
        user_message = f"""Given the following SQL tables, your job is to determine the columns and tables that the question is referring to.
        {database_schema}
        ###
        Question: {question}
        """
        messages = [
            {"role": "user", "content": user_message.strip()}
        ]
        inputs = self.tokenizer.apply_chat_template(
            messages, 
            return_tensors="pt",
            add_generation_prompt=True,
            tokenize = True).to(self.model.device)
        try:
            response, total_tokens = self.call_mistral(inputs)
            error_message = None
        except Exception as e:
            response = ""
            total_tokens = -1
            error_message = str(e)
        table_set = set() 
        if ";" in response:
            response = response.split(";")[0]
            if "Tables:" in response:
                response = response.split("Tables:")[1]
        response = re.sub(r'\s+', ' ', response).strip()
        for t_name in response.split(","):
            t_name = t_name.strip()
            try:
                columns = benchmark_question.schema.get_table_by_name(t_name).columns
            except KeyError:
                columns = []
            table_set.add(SchemaTable(
                name=t_name,
                columns=columns
            ))
        return SchemaSubsetterResult(
            schema_subset=Schema(
                database=benchmark_question.schema.database,
                tables=list(table_set)
            ),
            prompt_tokens=total_tokens,
            error_message=error_message
        )
    
