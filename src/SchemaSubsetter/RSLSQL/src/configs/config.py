import json
import os

dev_databases_path = './database/dev_databases'
dev_json_path = './data/dev.json'

# model='gpt-4o' #gpt-4o or deepseek-coder
model='gpt-4.1'

# Skalpel mod


print(os.getcwd())
with open(".local/openai.json") as f:
    api_key = json.loads(f.read())["api_key"]

api = api_key
base_url = 'https://api.openai.com/v1'
