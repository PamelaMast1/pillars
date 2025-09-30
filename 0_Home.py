import streamlit as st
import pandas as pd
import re
import base64
from PIL import Image
from datetime import timedelta
#from data_load import fetch_summary_workout_data, fetch_edge_workout_data, fetch_summary_workout_data_by_date, fetch_error_workout_data

from helpers.utils import get_first_day_of_last_month

# PAGE CONFIGS
st.set_page_config(
    page_title="Pillars of Data Engineering",
    page_icon=Image.open("assets/img/schemanest_logo.png"),
    layout="wide",
    initial_sidebar_state="expanded"
)


