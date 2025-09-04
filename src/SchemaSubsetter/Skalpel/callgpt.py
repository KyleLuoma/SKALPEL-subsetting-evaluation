import openai
import json
import time
import tiktoken

def call_gpt(
        prompt, 
        max_attempts = 10, 
        model = "gpt4.1-nano",
        max_tokens=800,
        verbose=True,
        delay_seconds=2
        ):
    try_again = True
    
    f = open('./.local/openai.json')
    openai_params = json.load(f)
    f.close()
    # openai.api_key = openai_params['api_key']
    client = openai.OpenAI(api_key=openai_params['api_key'])
    # openai.Model.list()
    num_attempts = 0
    while try_again and num_attempts < max_attempts:
        num_attempts += 1
        time.sleep(delay_seconds)
        try:
            response = client.chat.completions.create(
                    model=model,
                    messages=[
                    {
                        "role": "user", 
                        "content": prompt}
                    ],
                    temperature=openai_params['temperature'],
                    max_tokens=max_tokens,
                    top_p=openai_params['top_p'],
                    frequency_penalty=openai_params['frequency_penalty'],
                    presence_penalty=openai_params['presence_penalty'],
                    stop=["#", ";"]
                )
            try_again = False
        except Exception as e:
            print("Got an error, trying again...")
            print(e)
            try_again = True
            #wait for 10 seconds
            time.sleep(10)
            print("Trying again...")

    if verbose:
        print("\nGPT response:")
        for choice in response.choices:
            print(choice.message.content)
        print("\n")
    return response.choices[0].message.content, response.usage.total_tokens



def get_decoded_token_list(
        input: str,
        model: str = "gpt-3.5-turbo-16k"
) -> list:
    enc = tiktoken.encoding_for_model(model)
    try:
        tokens = enc.encode(input)
    except TypeError as e:
        print(e)
        return []
    decoded_tokens = [enc.decode([tok]) for tok in tokens]
    return decoded_tokens

if __name__ == "__main__":
    tokens = get_decoded_token_list("tlu_overstory")
    print(tokens)