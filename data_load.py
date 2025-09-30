import os
import json
import streamlit as st
#from google.oauth2 import service_account
#from google.cloud import bigquery
from datetime import datetime


# def get_google_credentials():

#     raw_cred = os.environ.get("GOOGLE_CREDENTIALS")

#     if raw_cred is None:
#         raise ValueError("GOOGLE_CREDENTIALS environment variable not set")

#     # Check if it's a file path
#     if os.path.exists(raw_cred):
#         with open(raw_cred) as f:
#             credentials_info = json.load(f)
#     else:
#         # Assume it's a JSON string
#         credentials_info = json.loads(raw_cred)

#     return service_account.Credentials.from_service_account_info(credentials_info)

# # All workout data for the customer
# #@st.cache_data
# def fetch_all_workout_data(start_date: datetime, end_date: datetime):

#     # test user for now
#     user_sk = st.secrets["TEST_USER_SK"]
#     if user_sk is None:
#         raise ValueError("TEST_USER_SK environment variable not set")
    
#     #credentials_info = json.loads(os.environ["GOOGLE_CREDENTIALS"])
#     credentials = get_google_credentials()

#     # Initialize BigQuery client
#     project = credentials.project_id
#     client = bigquery.Client(credentials=credentials, project=project)

#     start_str = start_date.strftime("%Y-%m-%d")
#     end_str = end_date.strftime("%Y-%m-%d")
    
#     # Define the SQL query
#     query = f"""
#     SELECT user_sk, 
#            workout_sk, 
#            length_minutes, 
#            class_title, 
#            total_output, 
#            avg_watts, 
#            avg_resistance, 
#            avg_cadence_rpm,
#            avg_speed_kmh, 
#            distance_km, 
#            calories_burned, 
#            avg_heartrate_bpm, 
#            avg_incline_percent, 
#            avg_pace_min_per_km, 
#            date, 
#            year, 
#            month, 
#            day, 
#            day_of_week, 
#            day_of_year, 
#            day_name, 
#            month_name, 
#            quarter, 
#            hour, 
#            minute, 
#            second, 
#            am_pm_indicator, 
#            hour_12_hour_format, 
#            fitness_discipline, 
#            instructor_pseudonym, 
#            fitness_type, 
#            fitness_sub_type, 
#            equipment
#     FROM `{project}.gold_lifestyle.mv_workout_data`
#     WHERE user_sk = '{user_sk}' AND 
#           DATE(date) BETWEEN DATE('{start_str}') AND DATE('{end_str}')
#     """
    
#     query_job = client.query(query)
#     df = query_job.result().to_dataframe()
#     return df


