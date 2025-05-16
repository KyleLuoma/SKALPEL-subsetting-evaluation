import os
import time
from tqdm import tqdm
import openai

# Modify your own openai config
# openai.api_base = os.environ["OPENAI_API_BASE"]
# openai.api_version = os.environ["OPENAI_API_VERSION"]
# openai.api_key = os.environ["OPENAI_API_KEY"]
openai.api_key = "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"

# Skalpel mod: adding model selection
model="gpt-4.1"

def connect_gpt4(message, prompt):
    
    response = openai.ChatCompletion.create(
                    model=model, 
                    messages = [{"role":"system","content":f"{message}"}, #"You are a helpful assisant. Help the user to complete SQL and no explaination is needed."
                                {"role":"user", "content":f"{prompt}"}],
                    temperature=0,
                    max_tokens=800, #800
                    top_p=1,
                    frequency_penalty=0,
                    presence_penalty=0,
                    stop = None)
    return response['choices'][0]['message']['content']

# Skalpel mod: updated openai call to newer version and added model selection parameter, returns token count
# Skalpel mod: Added handling for prompts that exceet context window
def collect_response(prompt, max_tokens = 800, stop = None, model = model) -> tuple[str, int]:
    while 1:
            flag = 0
            try:
                response = openai.chat.completions.create(
                    model=model, 
                    messages = [{"role":"system","content":"You are an AI assistant that helps people find information."}, #"You are a helpful assisant. Help the user to complete SQL and no explaination is needed."
                                {"role":"user", "content":f"{prompt}"}],
                    temperature=0,
                    max_tokens=max_tokens, #800
                    top_p=1,
                    frequency_penalty=0,
                    presence_penalty=0,
                    stop = stop)
                # response = response['choices'][0]['message']['content']
                response_text = response.choices[0].message.content
                response_tokens = response.usage.total_tokens
                flag = 1
                
            except Exception as e:
                print(e)
                if "string too long" in str(e) or "maximum context length" in str(e):
                    response_text = ""
                    response_tokens = -1
                    flag = 1
                time.sleep(1)
            if flag == 1:
                break
    return response_text, response_tokens

