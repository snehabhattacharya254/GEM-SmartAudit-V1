# This file has the end to end streamlit application however, it is known that this message is not the best

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
# Create a Bedrock Runtime client in the AWS Region you want to use.
client = boto3.client(
    service_name="bedrock-runtime",
    region_name=REGION_NAME,
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    aws_session_token=AWS_SESSION_TOKEN
    )
client = boto3.client('sts')

def retriever():
    return AmazonKnowledgeBasesRetriever(
        client=bedrock_knowledgebase,
        credentials_profile_name= None,
        knowledge_base_id="SOGVEXBGRX",  # Set your Knowledge base ID
        retrieval_config={"vectorSearchConfiguration": {"numberOfResults": 3}},
    )

# def run_chain(query):
#     # Initialize the retriever and model
#     retriever_callable = retriever()  # This should return a callable
#     model_callable = model()

#     # Check if the query is asking for a summary
#     prompt_to_use =  general_prompt
#     chain = (
#         RunnableParallel({
#             "context": itemgetter("question") | retriever_callable,
#             "question": itemgetter("question"),
#             "history": itemgetter("history"),
#         })
#         .assign(response = prompt_to_use | model_callable | StrOutputParser())
#         .pick(["response", "context"])
#     )

#     return chain

def get_iam_user_id():   
    # Get the caller identity
    identity = client.get_caller_identity()
   
    # Extract and return the User ID
    user_id = identity['UserId']
    return user_id

bedrock = boto3.client(service_name="bedrock-runtime",
                       region_name="us-east-1")
retriever_callable = retriever()
table_name = "SessionTable"
session_id = get_iam_user_id()  # You can make this dynamic based on the user session
history = DynamoDBChatMessageHistory(table_name=table_name, session_id=session_id)

st.title("GIA SmartAudit Bot")
if "messages" not in st.session_state:
    st.session_state.messages = []

    stored_messages = history.messages

    for msg in stored_messages:
        role = "user" if msg.__class__.__name__ == "HumanMessage" else "assistant"
        st.session_state.messages.append({"role": role, "content": msg.content})

# Display chat messages from history on app rerun
for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])

if prompt := st.chat_input("What is up?"):
    with st.chat_message("user"):
        st.markdown(prompt)

    # Define the user_message string with the prompt variable formatted into it
    user_message = [
        MessagesPlaceholder(variable_name="history"),  # Include chat history
        ("system", "You have been provided with audit reports that contain various attributes such as OpsAudit Organization, GIA AE Name, Fiscal Year, Issue ID, Title, New/Recurring issues, whether the risk is governed by policy or law, issue statement, risk description, root cause, supporting detail, recommendation, remediation action, involvement of other Takeda departments or parties, and relevant due dates."
         "The user may ask you to generate a detailed summary of an issue, including all sub-sections as requested. If there are multiple issues with the same name, include them all in the summary. Ensure that the language is formal and professional, suitable for presentation to the company's board of directors and stakeholders."
         "Below is the context: Answer the question based only on the following context: \n{context}"),
         ("human", "{question}"),
]

    # Update the prompt_chain with the new user_message that includes the prompt
    prompt = ChatPromptTemplate.from_messages(user_message)

    # Initialize the model (replace with actual model initialization code)
    model = ChatBedrockConverse(
    model="us.anthropic.claude-3-opus-20240229-v1:0",
    max_tokens=4096,
    temperature=0.0,
    top_p=1,
    # top_k=250,
    stop_sequences=["\n\nHuman"],
    verbose=True
    )
    
    def run_chain(query):
    # Initialize the retriever and model
        retriever_callable = retriever()  # This should return a callable

    # Check if the query is asking for a summary
        chain = (
            RunnableParallel({
            "context": itemgetter("question") | retriever(),
            "question": itemgetter("question"),
            "history": itemgetter("history"),
        })
        .assign(response = prompt | model | StrOutputParser())
        .pick(["response", "context"])
            )

        return chain 
    
    # chain = prompt_chain | model | StrOutputParser()
    chain_with_history = RunnableWithMessageHistory(
        run_chain(prompt),
        lambda session_id: DynamoDBChatMessageHistory(
            table_name="SessionTable", session_id=session_id
        ), # Reference DynamoDBChatMessageHistory
        input_messages_key="question",
        history_messages_key="history",
    )

    st.session_state.messages.append({"role": "user", "content": prompt})

    config = {"configurable": {"session_id": session_id}}
    response = chain_with_history.invoke({"question": prompt}, config=config)

    with st.chat_message("assistant"):
        st.markdown(response)

    st.session_state.messages.append({"role": "assistant", "content": response})
