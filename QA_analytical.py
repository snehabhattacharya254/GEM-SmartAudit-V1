from QA_data_cleaned import data_save_transform, output_transformed_chunk, summarize_each_record
from QA_data_cleaned import final_answer, summaries, clean_summary
import streamlit as st
import pandas as pd
import boto3
from botocore.exceptions import ClientError
import json
import os
import logging
from datetime import timedelta
from PIL import Image
import streamlit as st
from dotenv import load_dotenv
from langchain_community.chat_message_histories import DynamoDBChatMessageHistory
from langchain_core.prompts import ChatPromptTemplate, MessagesPlaceholder
from langchain_core.runnables.history import RunnableWithMessageHistory
from langchain_aws import ChatBedrockConverse
from langchain_core.output_parsers import StrOutputParser
from langchain_community.chat_message_histories import StreamlitChatMessageHistory
load_dotenv()

logging.basicConfig(level=logging.CRITICAL)

AWS_ACCESS_KEY_ID = st.secrets['AWS_ACCESS_KEY_ID']
AWS_SECRET_ACCESS_KEY = st.secrets['AWS_SECRET_ACCESS_KEY']
AWS_SESSION_TOKEN = st.secrets['AWS_SESSION_TOKEN']
REGION_NAME = st.secrets['REGION_NAME']

icon = Image.open("favicon-16x16.png")
# st.set_page_config(page_title="Takeda QA Analytical SmartAudit", page_icon=icon)
st.logo("takeda_logo.png")
avatar = Image.open('akiko.png')
st.header("Takeda QA Analytical SmartAudit 🤖")

st.markdown("The chatbot processes your query by running a backend code engine that extracts and transforms the data to focus on the most relevant columns and fields.")
st.markdown("A specialized model then summarizes the transformed data into a concise and comprehensive format.")
st.markdown("The final summary is displayed here in the interface for your review.")
st.markdown("Please note that this process may take up to 5 minutes, as it involves several detailed steps. Thank you for your patience!")

# the below is without history
if "messages" not in st.session_state:
    st.session_state.messages = []
    
history = StreamlitChatMessageHistory(key="chat_messages")
# Clear Chat History function
def clear_chat_history():
    history.clear()
    st.session_state.messages = [{"role": "assistant", 
                                  "content": "How may I assist you today?"}]

# Streamlit UI
with st.sidebar:
    st.title('Session Chat History')
    st.button('Clear Chat History', on_click=clear_chat_history, 
              help = "This clears chat history for the session. Click this when the history of the chat is not needed for the conversation.")
    st.divider()
    
for message in st.session_state.messages:
    with st.chat_message(message["role"], avatar=avatar):
        st.markdown(message["content"])

if prompt := st.chat_input("How can I help you?"):
    with st.chat_message("user"):
        st.markdown(prompt)
    with st.spinner("Please wait while I generate a response..."):
            
    # data transformation
        filename = data_save_transform(prompt)
        split_chunks = output_transformed_chunk(filename)
        summaries_response = summaries(prompt, split_chunks) # this function has to take in the user query to generate a summary for each chunk
        cleaned_summaries = [clean_summary(summary) for summary in summaries_response]
    
    # this is most likely the context needed to be stored in history
        combined_summary = "\n".join(cleaned_summaries)
    
        response = final_answer(prompt, combined_summary)
    
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("assistant", avatar=avatar):
        st.write(response)
    
    st.session_state.messages.append({"role": "assistant", "content": response})
    
def send_prompt_to_chatbot(prompt):
    # chain_var = run_chain(user_model, prompt)
    with st.chat_message("user"):
        st.markdown(prompt)
    with st.spinner("Please wait while I generate a response..."):
        filename = data_save_transform(prompt)
        split_chunks = output_transformed_chunk(filename)
        summaries_v2 = summaries(prompt, split_chunks) # this function has to take in the user query to generate a summary for each chunk
        cleaned_summaries = [clean_summary(summary) for summary in summaries_v2]
    
    # this is most likely the context needed to be stored in history
        combined_summary = "\n".join(cleaned_summaries)
    
        response = final_answer(prompt, combined_summary)
        
    st.session_state.messages.append({"role": "user", "content": prompt})
 
    with st.chat_message("assistant", avatar=avatar):
        st.write(response)
    
    st.session_state.messages.append({"role": "assistant", "content": response})
    
prompt_library = {
    "prompt1":"Please provide a summary of all Audit Country - Argentina audits with all details",
    "prompt2":"Using pr_id 1318886, identify the main areas of concern noted during the audit of Prof. Ludmila Tankova's site in Sofia, Bulgaria, for the SHP647-302 study conducted in october 2018. Summarize the actions taken to address these concerns, including any corrective measures implemented or recommended by the auditor, Julian Reeves, to ensure compliance with GCP guidelines and the clinical study protocol",
    "prompt3":"Could you please provide a detailed summary of the executive summary and audit results with pr_id 2753153?"
}
temp = 0
col1, col2 = st.columns(2, gap="medium")

with col1:
    if st.button("Please provide a summary of all Audit Country - Argentina audits with all details"):
        temp = 1
with col2:
    if st.button("For planning future audits, using pr_id 1318886, provide information on the audit execution (on-site), the audit type (Clinical and Medical Compliance), and any co-auditors involved. Also, include details on the audit distribution list to understand the stakeholders involved"):
        temp = 2

if temp == 1:
    send_prompt_to_chatbot(prompt_library['prompt1'])
if temp == 2:
    send_prompt_to_chatbot(prompt_library['prompt2'])
