import pandas as pd

from sqlalchemy import create_engine, text
from src.ETL import cleaned_data
import os

# Update with your actual database credentials
# Get database connection info from environment variables
db_host = os.getenv('DB_HOST', 'ep-dark-star-a4oefo1q-pooler.us-east-1.aws.neon.tech')
db_port = os.getenv('DB_PORT', '5432')
db_user = os.getenv('DB_USER', 'neondb_owner')
db_pass = os.getenv('DB_PASSWORD', 'npg_QXn1jCVf0yrg')
db_name = os.getenv('DB_NAME', 'neondb')
ssl_mode = os.getenv('SSL_MODE', 'require')

# Create connection string from environment variables
engine = f'postgresql://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}?sslmode={ssl_mode}'
student_dim = cleaned_data[[
    'Gender', 'School_Type', 'Distance_from_Home',
    'Parental_Education_Level', 'Family_Income'
]].drop_duplicates().reset_index(drop=True)

# Add an ID column manually (to avoid auto increment issues)
student_dim['student_id'] = student_dim.index + 1

# Rename columns to lowercase before loading
student_dim.columns = [col.lower() for col in student_dim.columns]

# Load to PostgreSQL
student_dim.to_sql('student_dim', engine, index=False, if_exists='replace')

parental_involvement_dim = cleaned_data[['Parental_Involvement']].drop_duplicates().reset_index(drop=True)
parental_involvement_dim.columns = ['involvement_level']
parental_involvement_dim.columns = [col.lower() for col in parental_involvement_dim.columns]

parental_involvement_dim.to_sql('parental_involvement_dim', engine, index=False, if_exists='append')

health_activity_dim = cleaned_data[[
    'Sleep_Hours', 'Physical_Activity', 'Motivation_Level'
]].drop_duplicates().reset_index(drop=True)

health_activity_dim['health_activity_id'] = health_activity_dim.index + 1
health_activity_dim.columns = [col.lower() for col in health_activity_dim.columns]

health_activity_dim.to_sql('health_activity_dim', engine, index=False, if_exists='append')

# Reload dimensions from DB - SQLAlchemy 2.0+ compatible approach
with engine.connect() as conn:
    student_dim_db = pd.read_sql_query(text('SELECT * FROM student_dim'), conn)
    health_activity_dim_db = pd.read_sql_query(text('SELECT * FROM health_activity_dim'), conn)

    # Convert cleaned_data column names to lowercase to match database columns
    cleaned_data.columns = [col.lower() for col in cleaned_data.columns]
    
    # Merge to get foreign keys
    fact_data = cleaned_data.merge(student_dim_db, on=[
        'gender', 'school_type', 'distance_from_home',
        'parental_education_level', 'family_income'
    ])

    fact_data = fact_data.merge(health_activity_dim_db, on=[
        'sleep_hours', 'physical_activity', 'motivation_level'
    ])

    # Use lowercase column names and include parental_involvement directly
    fact_table = fact_data[[
        'student_id', 'exam_score', 'hours_studied', 'attendance',
        'previous_scores', 'health_activity_id', 'parental_involvement'
    ]]
    
    # Rename parental_involvement to involvement_level without needing any merge
    fact_table = fact_table.rename(columns={'parental_involvement': 'involvement_level'})

    # No need to rename columns again since they're already lowercase
    # Just load the fact table
    fact_table.to_sql('student_performance_fact', engine, index=False, if_exists='append')








