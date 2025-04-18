from sqlalchemy import create_engine, text
import os

# Replace with your actual PostgreSQL connection details
# Get database connection info from environment variables
db_host = os.getenv('DB_HOST', 'ep-dark-star-a4oefo1q-pooler.us-east-1.aws.neon.tech')
db_port = os.getenv('DB_PORT', '5432')
db_user = os.getenv('DB_USER', 'neondb_owner')
db_pass = os.getenv('DB_PASSWORD', 'npg_QXn1jCVf0yrg')
db_name = os.getenv('DB_NAME', 'neondb')
ssl_mode = os.getenv('SSL_MODE', 'require')

# Create connection string from environment variables
engine = f'postgresql://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}?sslmode={ssl_mode}'
# First, check if tables exist and drop them to avoid conflicts
with engine.begin() as conn:  # Using begin() for transaction management
    # Drop tables if they exist
    conn.execute(text("DROP TABLE IF EXISTS student_performance_mart"))
    conn.execute(text("DROP TABLE IF EXISTS health_wellness_mart"))
    conn.execute(text("DROP TABLE IF EXISTS parental_resources_mart"))
    
    # Create and populate the student performance mart
    student_performance_mart_sql = """
    CREATE TABLE student_performance_mart AS
    SELECT
        s.student_id,
        s.gender,
        s.school_type,
        f.exam_score,
        f.hours_studied,
        f.attendance,
        f.previous_scores
    FROM student_performance_fact f
    JOIN student_dim s ON f.student_id = s.student_id;
    """
    conn.execute(text(student_performance_mart_sql))
    print("Student Performance Mart created successfully.")
    
    # Create and populate the health wellness mart
    health_wellness_mart_sql = """
    CREATE TABLE health_wellness_mart AS
    SELECT
        s.student_id,
        s.gender,
        h.sleep_hours,
        h.physical_activity,
        h.motivation_level,
        f.exam_score
    FROM student_performance_fact f
    JOIN student_dim s ON f.student_id = s.student_id
    JOIN health_activity_dim h ON f.health_activity_id = h.health_activity_id;
    """
    conn.execute(text(health_wellness_mart_sql))
    print("Health & Wellness Mart created successfully.")
    
    # Create and populate the parental resources mart
    parental_resources_mart_sql = """
    CREATE TABLE parental_resources_mart AS
    SELECT
        s.student_id,
        s.gender,
        f.exam_score,
        f.hours_studied,
        p.involvement_level
    FROM student_performance_fact f
    JOIN student_dim s ON f.student_id = s.student_id
    JOIN parental_involvement_dim p ON f.involvement_level = p.involvement_level;
    """
    conn.execute(text(parental_resources_mart_sql))
    print("Parental Resources Mart created successfully.")

# The transaction is automatically committed at the end of the with block
print("All data marts created and populated successfully.")



# SQL query to insert data into Health & Wellness Mart
health_wellness_mart_insert_sql = """
INSERT INTO health_wellness_mart (student_id, gender, sleep_hours, physical_activity, motivation_level, exam_score)
SELECT
    s.student_id,
    s.gender,
    h.sleep_hours,
    h.physical_activity,
    h.motivation_level,
    f.exam_score
FROM student_performance_fact f
JOIN student_dim s ON f.student_id = s.student_id
JOIN health_activity_dim h ON f.health_activity_id = h.health_activity_id;
"""

# Execute the insert query
with engine.connect() as conn:
    conn.execute(text(health_wellness_mart_insert_sql))
    print("Data inserted into Health & Wellness Mart successfully.")


# SQL query to insert data into Health & Wellness Mart
health_wellness_mart_insert_sql = """
INSERT INTO health_wellness_mart (student_id, gender, sleep_hours, physical_activity, motivation_level, exam_score)
SELECT
    s.student_id,
    s.gender,
    h.sleep_hours,
    h.physical_activity,
    h.motivation_level,
    f.exam_score
FROM student_performance_fact f
JOIN student_dim s ON f.student_id = s.student_id
JOIN health_activity_dim h ON f.health_activity_id = h.health_activity_id;
"""

# Execute the insert query
with engine.connect() as conn:
    conn.execute(text(health_wellness_mart_insert_sql))
    print("Data inserted into Health & Wellness Mart successfully.")


# SQL query to insert data into Parental Resources Mart
parental_resources_mart_insert_sql = """
INSERT INTO parental_resources_mart (student_id, gender, exam_score, hours_studied, involvement_level)
SELECT
    s.student_id,
    s.gender,
    f.exam_score,
    f.hours_studied,
    p.involvement_level
FROM student_performance_fact f
JOIN student_dim s ON f.student_id = s.student_id
JOIN parental_involvement_dim p ON f.involvement_level = p.involvement_level;
"""

# Execute the insert query
with engine.connect() as conn:
    conn.execute(text(parental_resources_mart_insert_sql))
    print("Data inserted into Parental Resources Mart successfully.")


