import json

def get_params():
    with open('pipeline_params.json') as json_file:  
        data = json.load(json_file)
    return data
