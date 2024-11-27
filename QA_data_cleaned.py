import pandas as pd
import boto3
import os
import time
from typing import List, Tuple
from dotenv import load_dotenv
import pandas as pd
import numpy as np
import time
from io import StringIO, BytesIO
import io
import re
import json
import streamlit as st
load_dotenv()

# # env variables
# AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
# AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
# AWS_SESSION_TOKEN = os.getenv('AWS_SESSION_TOKEN')
# REGION_NAME = os.getenv('REGION_NAME')

AWS_ACCESS_KEY_ID = st.secrets['AWS_ACCESS_KEY_ID']
AWS_SECRET_ACCESS_KEY = st.secrets['AWS_SECRET_ACCESS_KEY']
AWS_SESSION_TOKEN = st.secrets['AWS_SESSION_TOKEN']
REGION_NAME = st.secrets['REGION_NAME']

# Importing data from S3

from botocore.exceptions import ClientError

s3 = boto3.client('s3', region_name=REGION_NAME, aws_access_key_id=AWS_ACCESS_KEY_ID,
                  aws_secret_access_key=AWS_SECRET_ACCESS_KEY, aws_session_token=AWS_SESSION_TOKEN)

bucket_name = "ref-pr-detail-trkw-glbl"
object_key = "QA-data/smart_audits_trkw_audit_fields.xlsx"
data = pd.DataFrame()
file = s3.get_object(Bucket=bucket_name, Key=object_key)
data = file['Body'].read()
df = pd.read_excel(io.BytesIO(data))

# transposing the file
df_pivot = df.pivot_table(index='pr_id', columns='data_field_nm', values='field_value', 
                          aggfunc='first').reset_index()

data = df_pivot
client = boto3.client(service_name="bedrock-runtime",region_name=REGION_NAME,aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,aws_session_token=AWS_SESSION_TOKEN
    )
model_id = "anthropic.claude-v2" # code generating LLM
column_in_data = df_pivot.columns

# important columns to include for a summary
important_columns_for_summary = ['pr_id','Audit Category','Audit Classification (R&D)','Audit Conduct',
                                 'Audit Date - Performed (Start)','Audit Date - Performed (End)',
                                 'Audit Scheduled End Date','Audit Scheduled Start Date','Audit Scheduled by',
                                 'Audit Subtype (R&D)','Audit Subtype Category','Audit Type (R&D)','Description',
                                 'Executive Summary (R&D)','Program Lead','Regulatory Focus','Supporting QAC Group']


# transforming the data to produce a subset of relevant data

def data_save_transform(query):
    user_message = f"""Write a high-quality python script for the following task, something a very skilled python expert would write. You are writing code for an experienced developer so only add comments for things that are non-obvious. Make sure to include any imports required. 
    NEVER write anything before the ```python``` block. After you are done generating the code, check your work carefully to make sure there are no mistakes, errors, or inconsistencies. 
    Strictly no other text or instructions should be returned before or after the code. If they are added, please ensure they are comments.
    Here is the task:
    <task>
    The data is stored in a dataframe called 'df_pivot'. No need to load it as it is already loaded. Just use the 'df_pivot' dataframe when you are writing the code. The columns in the data here: {column_in_data}.
    Your task is to analyze the dataframe and define a function to answer the user's query: {query}. Some rows could contain NaN values, so please include na=False in a str.contains statement. Use def func() as the name of the function.
    Here are some important columns to consider for the result summary dataframe: {important_columns_for_summary}
    Generate a table with adequate information that contains the correct project ids and columns required respond to the query. And save the table to a variable.
    For example if the user asks: Summarize all the PV System & International audits. The code should be:
    def func():
        df = df_pivot[df_pivot['Audit Type (R&D)'].str.contains('PV System & International',na=False)]
        summary = df[important_columns_for_summary]
        return summary
    Another example if the user asks: Summarize all the not acceptable audits. The code should be:
    def func():
        df = df_pivot[df_pivot['Audit Classification (R&D')].str.contains('Not Acceptable', na=False)]
        summary = df[important_columns_for_summary]
        return summary
    </task>
    Only print inside the ```python``` block and no other text. Only include columns that are necessary to answer the user's query.
    """

    
    query = ""
    conversation = [
    {
        "role": "user",
        "content": [{"text": user_message}],
    }
    ]

    try:
    # Send the message to the model, using a basic inference configuration.
        response = client.converse(
            modelId="anthropic.claude-v2",
            messages=conversation,
            inferenceConfig={"maxTokens":2048,"stopSequences":["\n\nHuman:"],"temperature":0,"topP":1},
            additionalModelRequestFields={"top_k":250}
        )

    # Extract and print the response text.
        response_text = response["output"]["message"]["content"][0]["text"]
        # print(response_text)

    except (ClientError, Exception) as e:
        print(f"ERROR: Can't invoke '{model_id}'. Reason: {e}")
        exit(1)

    keyword = "python"
    before_keyword, keyword, after_keyword = response_text.partition(keyword)
    code = after_keyword.replace("```","")
    print(code)
    # execute code and save the resultant file as a json
    
    exec(code, globals())
    result = func()  # type: ignore
    file_name = "qa_json_temp" + str(round(time.time())) + ".json"
   
    if isinstance(result, (pd.DataFrame, list, dict)):
        # Convert result into a DataFrame
        result.to_json(f"{file_name}", orient = "records", date_format = "epoch", 
            double_precision = 10, force_ascii = True, date_unit = "ms", default_handler = None)
        # return "Saved the result output as a dataframe saved in json"
        return file_name
    else:
        result_json = json.dumps(result)
        with open(f"{file_name}", "w") as json_file:
                json_file.write(result_json)
        # return "Saved result as a json dump"
        return file_name

# query = "Summarize all the not acceptable audits based on audit classification."
# data_save_transform(query)

# Summarize the above information by first chunking them into approx 10 chunks

def output_transformed_chunk(file_name):
    with open(f'{file_name}') as f:
        data = json.load(f)
        def split_into_chunks(lst, n):
            for i in range(0, len(lst), n):
                yield lst[i:i+n]
    split_chunks = list(split_into_chunks(data, 10))
    return split_chunks

# The above two functions run this way:
# filename = data_save_transform(query) - returns file_name that is input for output_transformed_chunk
# split_chunks = output_transformed_chunk(filename) - return chunks

def summarize_each_record(query, chunk):

    model_id = "us.anthropic.claude-3-haiku-20240307-v1:0"
    user_message = f"""The following data is a very small subset of a bigger file containing audit issues that have been filtered from a previous LLM model after some data analysis.
    Your task is to provide an answer based on the information in the json file and if required analyze the results listed and answer the user's query with the most accuracy: {query}
    The data contained maybe very small or large. Please respond appropriately.
    <answer>
    {chunk}
    </answer>
    """
    conversation = [
    {
        "role": "user",
        "content": [{"text": user_message}],
    }
]

    try:
    # Send the message to the model, using a basic inference configuration.
        response = client.converse(
            modelId=model_id,
            messages=conversation,
            inferenceConfig={"maxTokens":4096,"temperature":0},
            additionalModelRequestFields={"top_k":250}
        )

    # Extract and print the response text.
        response_text = response["output"]["message"]["content"][0]["text"]
        return response_text

    except (ClientError, Exception) as e:
        return f"ERROR: Can't invoke '{model_id}'. Reason: {e}"
        exit(1)

# output summaries for each chunk. This requires the query to be as an input

def summaries(query, split_chunks):
    summaries = []
    for i, chunk in enumerate(split_chunks):
        try:
            response = summarize_each_record(query, chunk)
            summaries.append(response)
        except Exception as e:
            print(f"Error processing chunk {i+1}: {e}")
    return summaries

# Third function that helps to collate all summaries into one

def clean_summary(summary):
    # Define patterns to remove repetitive text
    patterns = [
        r"Based on the audit classification[,:]*",  # Example: "From the JSON file:"
        r"Based on the information provided in the JSON file"   # Example: "In this JSON chunk:"
    ]
    for pattern in patterns:
        summary = re.sub(pattern, "", summary, flags=re.IGNORECASE).strip()
    return summary

# Generating the final response

def final_answer(query, combined_summary):
    model_id = "us.anthropic.claude-3-haiku-20240307-v1:0"
    prompt = f"""
    Below given is a list of summaries for different chunks of data that another LLM model has summarized. 
    Please summarize the following consolidated information into a concise and comprehensive summary, and consider the user's query to fine-tune the response {query}.    
    {combined_summary}
    """
    
    # Call the LLM
    messages = [
    {
        "role": "user",
        "content": [{"text": prompt}],
    }
    ]

    try:
    # Send the message to the model, using a basic inference configuration.
        response = client.converse(
            modelId=model_id,
            messages=messages,
            inferenceConfig={"maxTokens":4096,"temperature":0},
            additionalModelRequestFields={"top_k":250}
        )
        response_text = response["output"]["message"]["content"][0]["text"]
        return response_text
    except Exception as e:
        return e



# filename = data_save_transform(query)
# split_chunks = output_transformed_chunk(filename)
# summaries = summaries(split_chunks)

# cleaned_summaries = [clean_summary(summary) for summary in summaries]
# combined_summary = "\n".join(cleaned_summaries)