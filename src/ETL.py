import pandas as pd
# Read CSV file into a pandas DataFrame
data = pd.read_csv("StudentPerformanceFactors.csv")

# Step 1: Handle Duplicates
data_transformed = data

# Check for missing values
print(data_transformed.isnull().sum())

# Step 2: Remove rows with nulls in important columns
columns_with_nulls_to_drop = [
    'Teacher_Quality', 'Parental_Education_Level', 'Distance_from_Home'
]
cleaned_data_no_nulls = data_transformed.dropna(subset=columns_with_nulls_to_drop).reset_index(drop=True)

print(cleaned_data_no_nulls.isnull().sum())

# Step 3: Standardize Categorical Columns

# Convert only string columns to lowercase
for col in cleaned_data_no_nulls.columns:
    if cleaned_data_no_nulls[col].dtype == 'object':
        cleaned_data_no_nulls[col] = cleaned_data_no_nulls[col].str.lower()


# Step 4: Drop unused columns — keep only relevant ones

columns_to_keep = [
    'Gender', 'School_Type', 'Distance_from_Home', 'Parental_Education_Level', 'Family_Income',
    'Parental_Involvement', 'Sleep_Hours', 'Physical_Activity', 'Motivation_Level',
    'Exam_Score', 'Hours_Studied', 'Attendance', 'Previous_Scores'
]

cleaned_data = cleaned_data_no_nulls[columns_to_keep].reset_index(drop=True)

# Optional: Show cleaned data (only if running in an environment like Jupyter or ACE)
# Preview the cleaned dataset
print("\nCleaned & Transformed Data:")
print(cleaned_data.head())
