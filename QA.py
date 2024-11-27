import streamlit as st
from langchain_community.chat_message_histories import StreamlitChatMessageHistory
from QA_chain import run_chain, extract_citations, create_presigned_url, parse_s3_uri,history_chain,chain_citation
import streamlit as st
import time
from dotenv import load_dotenv
import os
import boto3
from datetime import timedelta
from PIL import Image
load_dotenv()

# AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
# AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
# AWS_SESSION_TOKEN = os.getenv('AWS_SESSION_TOKEN')
# REGION_NAME = os.getenv('REGION_NAME')

AWS_ACCESS_KEY_ID = st.secrets['AWS_ACCESS_KEY_ID']
AWS_SECRET_ACCESS_KEY = st.secrets['AWS_SECRET_ACCESS_KEY']
AWS_SESSION_TOKEN = st.secrets['AWS_SESSION_TOKEN']
REGION_NAME = st.secrets['REGION_NAME']

s3 = boto3.client('s3',
                  region_name= "us-east-1",
                  aws_access_key_id=AWS_ACCESS_KEY_ID,
                  aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                  aws_session_token=AWS_SESSION_TOKEN)

my_bucket = "ref-pr-detail-trkw-glbl"
my_prefix = "QA-kb-output3"

icon = Image.open("favicon-16x16.png")
# st.set_page_config(page_title="Takeda GIA SmartAudit", page_icon=icon)
st.logo("takeda_logo.png")
avatar = Image.open('akiko.png')

st.header("Takeda QA SmartAudit 🤖")
# Clear Chat History function
history = StreamlitChatMessageHistory(key="chat_messages")
# Clear Chat History function
def clear_chat_history():
    history.clear()
    st.session_state.messages = [{"role": "assistant", "content": "How may I assist you today?"}]

# Streamlit UI
with st.sidebar:
    st.title('Session Chat History')
    st.button('Clear Chat History', on_click=clear_chat_history, 
              help = "This clears chat history for the session. Click this when the history of the chat is not needed for the conversation.")
    st.divider()

# Initialize session state for messages if not already present
if "messages" not in st.session_state:
    st.session_state.messages = [{"role": "assistant", "content": "How may I assist you today?"}]

# Display chat messages
for message in st.session_state.messages:
    with st.chat_message(message["role"], avatar=avatar):
        st.write(message["content"])

if prompt := st.chat_input():
    # chain_with_history = run_chain(prompt, history)
    chain_var = run_chain(prompt)
    chain_with_history = history_chain(chain_var,prompt)
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.write(prompt)
    config = {"configurable": {"session_id": "any"}}
 
    with st.chat_message("assistant", avatar=avatar):
        response = chain_with_history.invoke(
            {"question": prompt, "history": history},
            {"configurable": {"session_id": "any"}}
        )
        st.write(response['response'])
        # Assuming extract_citations is a function that extracts citations from the response
        # citations = extract_citations(response['context'])
        citations = extract_citations(response['context'])
        with st.expander(label="Show source details >", expanded=False):
            for citation in citations:
                    # st.write("Page Content:", citation.page_content)
                s3_uri = citation.metadata['location']['s3Location']['uri']
                bucket, key = parse_s3_uri(s3_uri)
                presigned_url = create_presigned_url(bucket, key)
                if presigned_url:
                    st.markdown(f"Source: [{s3_uri}]({presigned_url})")
                else:
                    st.write(f"Source: {s3_uri} (Presigned URL generation failed)")
                st.write("Score:", citation.metadata['score'])
        st.session_state.messages.append({"role": "assistant", "content": response['response']})

def send_prompt_to_chatbot(prompt):
    # chain_var = run_chain(user_model, prompt)
    chain_var = run_chain(prompt)
    chain_with_history = history_chain(chain_var,prompt)
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.write(prompt)
    config = {"configurable": {"session_id": "any"}}
 
    with st.chat_message("assistant", avatar = avatar):
        response = chain_with_history.invoke(
            {"question": prompt, "history": history},
            {"configurable": {"session_id": "any"}}
        )
        st.write(response['response'])
    citations = extract_citations(response['context'])
    with st.expander(label="Show source details >", expanded=False):
        for citation in citations:
                    # st.write("Page Content:", citation.page_content)
            s3_uri = citation.metadata['location']['s3Location']['uri']
            bucket, key = parse_s3_uri(s3_uri)
            presigned_url = create_presigned_url(bucket, key)
            if presigned_url:
                st.markdown(f"Source: [{s3_uri}]({presigned_url})")
            else:
                st.write(f"Source: {s3_uri} (Presigned URL generation failed)")
            st.write("Score:", citation.metadata['score'])
    st.session_state.messages.append({"role": "assistant", "content": response['response']})


# dictionary of prompts
prompt_library = {
    "prompt1":"Referencing pr_id 1318886, provide detailed information on the audit conducted by Julian Reeves for Shire Human Genetic Therapies, Inc. (Shire) on Prof. Ludmila Tankova's investigator site. Include the audit's purpose, scope, methodology, and any findings or observations made",
    "prompt2":"For planning future audits, using pr_id 1318886, provide information on the audit execution (on-site), the audit type (Clinical and Medical Compliance), and any co-auditors involved. Also, include details on the audit distribution list to understand the stakeholders involved",
    "prompt3":"Could you please provide a detailed summary of the executive summary and audit results with pr_id 2753153?"
}
temp = 0
col1, col2, col3 = st.columns(3, gap="medium")

with col1:
    if st.button("Referencing pr_id 1318886, provide detailed information on the audit conducted by Julian Reeves for Shire Human Genetic Therapies, Inc. (Shire) on Prof. Ludmila Tankova's investigator site. Include the audit's purpose, scope, methodology, and any findings or observations made"):
        temp = 1
with col2:
    if st.button("For planning future audits, using pr_id 1318886, provide information on the audit execution (on-site), the audit type (Clinical and Medical Compliance), and any co-auditors involved. Also, include details on the audit distribution list to understand the stakeholders involved"):
        temp = 2
with col3:
    if st.button("Could you please provide a detailed summary of the executive summary and audit results with pr_id 2753153?"):
        temp = 3
        
if temp == 1:
    send_prompt_to_chatbot(prompt_library['prompt1'])
if temp == 2:
    send_prompt_to_chatbot(prompt_library['prompt2'])
if temp == 3:
    send_prompt_to_chatbot(prompt_library['prompt3'])




