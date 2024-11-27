import streamlit as st
import time
# from dotenv import load_dotenv
import os
import boto3
from datetime import timedelta
from PIL import Image
# load_dotenv()

# AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
# AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
# AWS_SESSION_TOKEN = os.getenv('AWS_SESSION_TOKEN')
# REGION_NAME = os.getenv('REGION_NAME')

AWS_ACCESS_KEY_ID = st.secrets['AWS_ACCESS_KEY_ID']
AWS_SECRET_ACCESS_KEY = st.secrets['AWS_SECRET_ACCESS_KEY']
AWS_SESSION_TOKEN = st.secrets['AWS_SESSION_TOKEN']
REGION_NAME = st.secrets['REGION_NAME']

icon = Image.open(r"favicon-16x16.png")
st.set_page_config(page_title="Takeda SmartAudit", page_icon=icon)
st.logo(r"takeda_logo.png")
avatar = Image.open(r'akiko.png')

# Adding QA pages here as sub-pages
GIA = st.Page("GIA.py", title = "Takeda GIA SmartAudit", icon = "🤖")
QA = st.Page("QA.py", title = "Takeda QA SmartAudit", icon="🤖")
QA_analytical = st.Page("QA_analytical.py", title = "Takeda QA Analytical SmartAudit", icon="🤖")

pg = st.navigation(
        {
            "QA": [QA, QA_analytical],
            "GIA":[GIA]
        }
)

pg.run()