import requests
import json
import time
from SchemaSubsetter.Skalpel import callgpt
from transformers import AutoTokenizer
import google.generativeai as genai
from google.generativeai.types import generation_types
from google.api_core.exceptions import InternalServerError
import json
from google.cloud import aiplatform
aiplatform.init(project='nl-to-sql-model-eval')


class LLM:
    
    def __init__(self):
        self.tokenizer = AutoTokenizer.from_pretrained("openai-community/gpt2-xl")

    def call_llm(self, prompt: str, model: str = None) -> tuple[str, int]:
        raise NotImplementedError
    
    def extract_json_from_response(
            self, 
            response: str, 
            expected_type: str = None,
            model: str = None
            ) -> object:
        if not model:
            model = OpenAIRequestLLM.DEFAULT_MODEL
        max_tries = 5
        num_tries = 0
        error_message = None
        while num_tries < max_tries:
            if "```json" in response:
                response = response.split("```json")[-1].split("```")[0]
            try:
                json_obj = json.loads(response)
                return json_obj
            except Exception as e:
                num_tries += 1
                error_message = str(e)
            response = self.repair_response_json(response, error_message, expected_type, model)
        if expected_type == "list":
            return []
        elif expected_type == "dict":
            return {}
        elif expected_type == "set":
            return set()
        return []


    def repair_response_json(
            self, 
            bad_response: str, 
            error_message: str, 
            expected_type: str, 
            model: str
            ) -> str:
        print("Attempting to repair LLM-generated JSON", bad_response, error_message)
        prompt = """
When trying to parse this JSON object: {bad_response} 
the python json library raised the following exception:
{error_message}.
The object type should be: {expect_type}
Please correct the error and return a valid json encased in ```json object goes here ```
Do your best to make the correction!
""".format(bad_response=bad_response, error_message=error_message, expect_type=expected_type)
        repaired, tokens = self.call_llm(prompt=prompt, model=model)
        return repaired
    


class VertexLLM(LLM):

    def __init__(
            self,
            api_config_file: str="./.local/vertex.json"
            ):
        super().__init__()
        with open(api_config_file) as f:
            vertex_info = json.loads(f.read())
        self.generation_config = {
            "temperature": vertex_info["temperature"],
            "top_p": vertex_info["top_p"],
            "top_k": vertex_info["top_k"],
            "max_output_tokens": vertex_info["max_tokens"],
        }
        genai.configure(api_key=vertex_info["api_key"])
        self.safety_settings = [
            {
                "category": "HARM_CATEGORY_HARASSMENT",
                "threshold": "BLOCK_NONE"
            },
            {
                "category": "HARM_CATEGORY_HATE_SPEECH",
                "threshold": "BLOCK_NONE"
            },
            {
                "category": "HARM_CATEGORY_SEXUALLY_EXPLICIT",
                "threshold": "BLOCK_NONE"
            },
            {
                "category": "HARM_CATEGORY_DANGEROUS_CONTENT",
                "threshold": "BLOCK_NONE"
            },
        ]

    def call_llm(
        self,
        prompt: str,
        model: str = None
    ) -> tuple[str, int]:
        if model == None:
            model = "gemini-2.0-flash-lite-001"
        model = genai.GenerativeModel(model_name=model,
                                    generation_config=self.generation_config,
                                    safety_settings=self.safety_settings)
        
        try_again = True
        num_tries = 0
        while try_again and num_tries < 5:
            try:
                result = model.generate_content(prompt)
                try_again = False
            except generation_types.StopCandidateException as e:
                print(e)
                if hasattr(e, 'message'):
                    return e.message
                else:
                    return "Encountered exception without a message attribute."
            except InternalServerError as e:
                time.sleep(3)
            num_tries += 1
        return result.text, result.usage_metadata.total_token_count




class OpenAIRequestLLM(LLM):
    REQUEST_URL = "https://wire.westpoint.edu/vllm/v1/chat/completions"
    DEFAULT_MODEL = "openai/gpt-oss-120b"

    def __init__(
            self, 
            request_url: str = None,
            api_config_file: str = "./.local/openai.json"
            ):
        super().__init__()
        if not request_url:
            self.request_url = OpenAIRequestLLM.REQUEST_URL
        else:
            self.request_url = request_url
        with open(api_config_file, "r") as f:
            self.api_key = json.load(f).get("api_key")
        



    def call_llm(self, prompt: str, model: str = None) -> tuple[str, int]:
        if not model:
            model = OpenAIRequestLLM.DEFAULT_MODEL
        if model != OpenAIRequestLLM.DEFAULT_MODEL and "gpt" in model:
            response_text, tokens_used = callgpt.call_gpt(
                prompt=prompt, 
                max_tokens=32768,
                model=model, 
                verbose=False
                )
        else:
            response_text, tokens_used = self.call_with_requests(prompt=prompt, model=model)
        return response_text, tokens_used



    def call_with_requests(self, prompt: str, model: str) -> tuple[str, int]:
        max_tries = 5
        num_tries = 0
        verify = "wire.westpoint.edu" not in self.request_url
        while num_tries < max_tries:
            headers = {
                "Content-Type": "application/json",
                # "Authorization": f"Bearer {self.api_key}"
            }
            response = requests.post(
                self.request_url,
                json = {
                    "model": model,
                    "messages": [
                    {
                        "role": "developer",
                        "content": "You are a data integration specialist evaluating a legacy system."
                    },
                    {
                        "role": "user",
                        "content": prompt
                    }
                    ],
                    "temperature": 0,
                    # "max_completion_tokens": 32768
                },
                verify=verify,
                headers=headers
            )
            if response.status_code == 200:
                return response.json()["choices"][0]["message"]["content"], response.json()["usage"]["total_tokens"]
            num_tries += 1
            time.sleep(2)
        return "", -1
    

    def get_prompt_token_count(self, prompt: str) -> int:
        return len(self.tokenizer.encode(prompt))
    


class Llama3LLM(LLM):

    def __init__(
            self,
            api_config_file: str="./.local/ollama.json"
            ):
        super().__init__()
        with open(api_config_file, "r") as f:
            ollama_info = json.loads(f.read())
            self.api_url = ollama_info["api_url"]
            self.model = ollama_info["model"]
            self.max_tokens = ollama_info["max_tokens"]
            self.temperature = ollama_info["temperature"]
            self.top_p = ollama_info["top_p"]
            self.top_k = ollama_info["top_k"]
            self.frequency_penalty = ollama_info["frequency_penalty"]
            self.presence_penalty = ollama_info["presence_penalty"]

    

    def call_llm(
            self, 
            prompt: str, 
            model: str = None,
            max_request_attempts: int = 5
            ) -> tuple[str, int]:
        request_params = {
            "model": self.model,
            "prompt": prompt,
            "max_tokens": self.max_tokens,
            "temperature": self.temperature,
            "top_p": self.top_p,
            "top_k": self.top_k,
            "frequency_penalty": self.frequency_penalty,
            "presence_penalty": self.presence_penalty,
            "stream": False
        }
        if model != None:
            request_params["model"] = model
        request_counter = 0
        while request_counter < max_request_attempts:
            response = requests.post(
                url=f"{self.api_url}/generate",
                json=request_params,
                verify=False
                )
            if response.status_code == 200:
                rj = response.json()
                return rj["response"], (rj["prompt_eval_count"] + rj["eval_count"])
            time.sleep(5)
        response.raise_for_status()




class LLMFactory:

    SUBSTRING_LLM_MAP = {
        "gemini": VertexLLM,
        "gpt": OpenAIRequestLLM,
        "llama3": Llama3LLM
    }

    @classmethod
    def build_llm_for_model(llm_fact, model_name: str) -> LLM:
        for k in llm_fact.SUBSTRING_LLM_MAP.keys():
            if k in model_name:
                llm = llm_fact.SUBSTRING_LLM_MAP[k]
                return llm()
        raise KeyError(model_name)