import boto3
from dotenv import load_dotenv
import os
load_dotenv()

AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_SESSION_TOKEN = os.getenv('AWS_SESSION_TOKEN')
REGION_NAME = os.getenv('REGION_NAME')

client = boto3.client('bedrock-agent-runtime', 
                      region_name=REGION_NAME,
                      aws_access_key_id=AWS_ACCESS_KEY_ID,
                      aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                      aws_session_token=AWS_SESSION_TOKEN)

kb_id = "SOGVEXBGRX" # GIA knowledge base
model_arn = "arn:aws:bedrock:us-east-1::foundation-model/anthropic.claude-3-haiku-20240307-v1:0"

def retrieved_generated(input):
    response = client.retrieve_and_generate(
        input = {
            'text':input
        },
        retrieveAndGenerateConfiguration = {
            'type':'KNOWLEDGE_BASE',
            'knowledgeBaseConfiguration':{
                'knowledgeBaseId': "SOGVEXBGRX",
                'modelArn': "arn:aws:bedrock:us-east-1::foundation-model/anthropic.claude-3-haiku-20240307-v1:0"
            }
        }
    )
    return response

# response = retrieved_generated('Provide details of IT Resiliency along with the issue ID, action plan owner, etc.')
response = retrieved_generated('Provide details for issue fixed asset management')

print(response['output']['text'])