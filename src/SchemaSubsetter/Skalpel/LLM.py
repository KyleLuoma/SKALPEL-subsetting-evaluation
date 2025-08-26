import requests
import json
import time
from SchemaSubsetter.Skalpel import callgpt

class AbstractLLM:
    pass

class OpenAIRequestLLM():
    REQUEST_URL = "https://wire.westpoint.edu/vllm/v1/chat/completions"
    DEFAULT_MODEL = "'openai/gpt-oss-120b'"

    def __init__(self, request_url: str = None):
        if not request_url:
            self.request_url = OpenAIRequestLLM.REQUEST_URL
        else:
            self.request_url = request_url
        with open("./.local/openai.json", "r") as f:
            self.api_key = json.load(f).get("api_key")



    def call_llm(self, prompt: str, model: str = None) -> tuple[str, int]:
        if not model:
            model = OpenAIRequestLLM.DEFAULT_MODEL
        if "wire.westpoint.edu" not in self.request_url:
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