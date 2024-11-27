import boto3
import logging
from dotenv import load_dotenv
import os
import streamlit as st
from typing import List, Dict
from pydantic import BaseModel
from operator import itemgetter
from langchain_core.prompts import ChatPromptTemplate, MessagesPlaceholder
from langchain_core.runnables import RunnablePassthrough, RunnableParallel
from langchain_core.output_parsers import StrOutputParser
from langchain_core.runnables.history import RunnableWithMessageHistory
from langchain_aws import ChatBedrock, ChatBedrockConverse
from langchain_aws import AmazonKnowledgeBasesRetriever
from langchain_community.chat_message_histories import StreamlitChatMessageHistory
# instantiating knowledge base
load_dotenv()

AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_SESSION_TOKEN = os.getenv('AWS_SESSION_TOKEN')
REGION_NAME = os.getenv('REGION_NAME')

bedrock_model = boto3.client(
    service_name="bedrock-runtime",
    region_name=REGION_NAME,
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    aws_session_token=AWS_SESSION_TOKEN
    )

# Building the chain

claude_3_5_sonnet = "anthropic.claude-3-5-sonnet-20240620-v1:0" 

# anthropic.claude-3-sonnet-20240229-v1:0

logging.getLogger().setLevel(logging.ERROR)

def model():
    return ChatBedrockConverse(client = bedrock_model,
                                   model_id = claude_3_5_sonnet,
                                   max_tokens = 2000,
                                   temperature = 0,
                                   )
# LangChain - RAG chain with citations
summary_messages = [
    ("system", "You have been provided with audit reports that contain various attributes. "
     "The user may ask you to generate a detailed summary of a pr_id or an audit classification, or an audit category, etc., please provide all details including all sub-sections as requested. If there are multiple pr_ids that are similar, include them all in the summary."
     "Ensure that the language is formal and professional, suitable for presentation to the company's board of directors and stakeholders."
     "Strictly do not hallucinate any details."
     "Below is the context: Answer the question based only on the following context: \n{context}"),
    MessagesPlaceholder(variable_name="history"),  # Include chat history
    ("system", "Answer the question based only on the following context:\n{context}"),
    ("human", "{question}"),
]
general_messages = [
    MessagesPlaceholder(variable_name="history"),  # Include chat history
    ("system", "You are an analytical assistant who has been given list of audit reports that are identifiable by pr_id. When a user gives a query, provide a precise answer to the query. Give the most importance to precision."
     "If the user's question has a pr_id or an audit classification, or a category, etc., check only that specific metadata file and no other files"
     "The user may ask you to provide differences between issues, focus on the differences between them, and provide all details of the issues as bullet points under sub-headings. Give the most importance to precision."
     "Concise Summary: Request a clear and concise summary to ensure the model provides the information in an easily digestible format."
     "Do not Hallucinate. Answer the question based only on the following context: \n{context}"),
    ("human", "{question}"),
]

summary_prompt = ChatPromptTemplate.from_messages(summary_messages)
general_prompt = ChatPromptTemplate.from_messages(general_messages)

# Initialize the AWS client for the Bedrock Runtime service
bedrock_knowledgebase = boto3.client(
    service_name="bedrock-agent-runtime",
    region_name=REGION_NAME,
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    aws_session_token=AWS_SESSION_TOKEN
)

# Define the retriever function
def retriever():
    return AmazonKnowledgeBasesRetriever(
        client=bedrock_knowledgebase,
        credentials_profile_name= None,
        knowledge_base_id="9D2XVGJFVI",  # Set your Knowledge base ID
        retrieval_config={"vectorSearchConfiguration": {"numberOfResults": 3,
                                                        'overrideSearchType': "HYBRID"}},
    )

# Define a function to check if the query is asking for a summary
def is_summary_request(query):
    summary_keywords = ['summarize', 'summary', 'sum up', 'condense', 'elaborate', 'Summarize',
                        'Summary', 'Sum', 'Elaborate']
    return any(keyword in query.lower() for keyword in summary_keywords)

class Citation(BaseModel):
    page_content: str
    metadata: Dict
    
def extract_citations(context: List[Dict]) -> List[Citation]:
    return [Citation(page_content=doc.page_content, metadata=doc.metadata) for doc in context]


def run_chain(query):
    # Initialize the retriever and model
    retriever_callable = retriever()  # This should return a callable
    model_callable = model()
    # Check if the query is asking for a summary
    prompt_to_use = summary_prompt if is_summary_request(query) else general_prompt
    chain = (
        RunnableParallel({
            "context": itemgetter("question") | retriever_callable,
            "question": itemgetter("question"),
            "history": itemgetter("history"),
        })
        .assign(response = prompt_to_use | model_callable | StrOutputParser())
        .pick(["response", "context"])
    )

    return chain

def history_chain(chain,query):
    history = StreamlitChatMessageHistory(key="chat_messages")    
    # Create a chain with history handling
    chain_with_history = RunnableWithMessageHistory(
        chain,
        lambda session_id: history,
        input_messages_key="question",
        history_messages_key="history",
        output_messages_key="response",
    )
    return chain_with_history

def chain_citation(chain,query):
    citation_response = chain.invoke(query)
    citations = extract_citations(citation_response['context'])
    return citations

def create_presigned_url(bucket_name: str, object_name: str, expiration: int = 300) -> str:
    """Generate a presigned URL to share an S3 object"""
    s3_client = boto3.client('s3')
    response_s3 = s3_client.generate_presigned_url('get_object',
                                                    Params={'Bucket': bucket_name,
                                                            'Key': object_name},
                                                    ExpiresIn=expiration)
    return response_s3

def parse_s3_uri(uri: str) -> tuple:
    """Parse S3 URI to extract bucket and key"""
    parts = uri.replace("s3://", "").split("/")
    bucket = parts[0]
    key = "/".join(parts[1:])
    return bucket, key
