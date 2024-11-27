
import pandas as pd
import boto3
from botocore.exceptions import ClientError
import json
import os
from langchain_aws import AmazonKnowledgeBasesRetriever
import logging
import streamlit as st
from dotenv import load_dotenv
from langchain_community.chat_message_histories import DynamoDBChatMessageHistory
from langchain_core.prompts import ChatPromptTemplate, MessagesPlaceholder
from langchain_core.runnables import RunnablePassthrough, RunnableParallel
from langchain_core.runnables.history import RunnableWithMessageHistory
from langchain_aws import ChatBedrockConverse
from operator import itemgetter
from langchain_core.output_parsers import StrOutputParser
from langchain_core.runnables import RunnableLambda
load_dotenv()
logging.basicConfig(level=logging.CRITICAL)

AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_SESSION_TOKEN = os.getenv('AWS_SESSION_TOKEN')
REGION_NAME = os.getenv('REGION_NAME')

bedrock_knowledgebase = boto3.client(
    service_name="bedrock-agent-runtime",
    region_name=REGION_NAME,
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    aws_session_token=AWS_SESSION_TOKEN
)

def retriever():
    return AmazonKnowledgeBasesRetriever(
        client=bedrock_knowledgebase,
        credentials_profile_name= None,
        knowledge_base_id="SOGVEXBGRX",  # Set your Knowledge base ID
        retrieval_config={"vectorSearchConfiguration": {"numberOfResults": 3}},
    )

user_message = [
    MessagesPlaceholder(variable_name="history"),  # Include chat history
    ("system", "You have been provided with audit reports that contain various attributes. "
     "The user may ask you to generate a detailed summary of an issue, including all sub-sections as requested. If there are multiple issues with the same name, include them all in the summary. Ensure that the language is formal and professional, suitable for presentation to the company's board of directors and stakeholders."
     "Below is the context: Answer the question based only on the following context: \n{context}"),
    ("human", "{question}"),
]

# Create the prompt template
prompt_template = ChatPromptTemplate.from_messages(user_message)
def model():
    return ChatBedrockConverse(
    model="us.anthropic.claude-3-opus-20240229-v1:0",
    max_tokens=4096,
    temperature=0.0,
    top_p=1,
    stop_sequences=["\n\nHuman"],
    verbose=True
    )

def run_chain(query, session_id):

    chain = (
        RunnableParallel({
         "context": itemgetter("question") | retriever(),  # Use retriever with the query
            "question": itemgetter("question"),  # Pass the raw question
            "history": itemgetter("history"),  # Include chat history
        })
        .assign(response=prompt_template | model() | StrOutputParser())  # Combine prompt and model
        .pick(["response", "context"])  # Extract desired outputs
    )
    
    chain_with_history = RunnableWithMessageHistory(
        chain,
        lambda session_id: DynamoDBChatMessageHistory(
            table_name="SessionTable", session_id=session_id), 
        input_messages_key="question",
        history_messages_key="history",
    )
    # Save the new history to DynamoDB
    
    
    # Return the response and context
    return chain_with_history.invoke({"question": query, "history": []}, 
                                      config = {'configurable': {'session_id': session_id}})

def get_iam_user_id():   
    # Get the caller identity
    client = boto3.client('sts')
    identity = client.get_caller_identity()
   
    # Extract and return the User ID
    user_id = identity['UserId']
    return user_id


session_id = get_iam_user_id()
query = "Mention the fixed asset management issues"

print(run_chain(query, session_id))