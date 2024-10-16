import logging
import azure.functions as func
import requests
import pandas as pd
import time
import re
import json
import os

'''
This module contains functions and a scheduled Azure Function to fetch, process, and save clinical trial data from the ClinicalTrials.gov API.
Functions:
    refresh_data_clinicalTrials(myTimer: func.TimerRequest) -> None:
        Azure Function triggered by a timer to refresh clinical trial data.
    check_api(next_page: str, retry_count: int = 0) -> tuple:
    get_data(response: requests.Response) -> tuple:
    extract_age_and_unit(age_str: str) -> tuple:
    homogenize_sponsor(sponsor_name: str) -> str:
    classify_by_keywords(row: pd.Series, keyword_dict: dict, columns: list) -> str:
    preprocess(df: pd.DataFrame) -> pd.DataFrame:
    csv_save(df: pd.DataFrame, df_name: str) -> None:
        Save the given DataFrame to a CSV file.
    main() -> None:
'''

app = func.FunctionApp()

@app.schedule(schedule="0 0 8 1 * *", arg_name="myTimer", run_on_startup=True, # set to run every 1st day of the month at 8:00 AM
              use_monitor=False) 
def refresh_data_clinicalTrials(myTimer: func.TimerRequest) -> None:
    if myTimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function executed.')
    main()
    
pd.options.mode.copy_on_write = False

nextPage = 'START'
processed_data = []
MAX_RECORDS = 10000  # Define the maximum number of records to download
MAX_RETRIES = 5  # Define the maximum number of retries in case of an error
RETRY_DELAY = 5  # Define the wait time in seconds before retrying the request

# List of sponsors to filter the data
SPONSORS = ['Novo Nordisk', 'Pfizer', 'Takeda', 'MSD', 'Merck Sharp & Dohme', 'Novartis', 'Astrazeneca', 'Bayer', 'Abbvie', 'Amgen', 'Bristol', 'Glaxosmithkline', 'Janssen', 'Roche']

# List of files and corresponding variable names
FILES_AND_VARIABLES = [
    ('condition_keywords.json', 'condition_keywords'),
    ('genetic_keywords.json', 'genetic_keywords'),
    ('advanced_therapies_keywords.json', 'advanced_therapies_keywords'),
    ('cancer_keywords.json', 'cancer_keywords'),
    ('rare_diseases_keywords.json', 'rare_diseases_keywords'),
    ('diabetes_keywords.json', 'diabetes_keywords')
]

# Define the path to the folder containing the JSON files
json_folder_path = os.path.join(os.path.dirname(__file__), 'Keywords_dictionaries')

# Load keywords from JSON files with proper encoding
for file_name, var_name in FILES_AND_VARIABLES:
    file_path = os.path.join(json_folder_path, file_name)
    with open(file_path, 'r', encoding='utf-8') as f:
        globals()[var_name] = json.load(f)
        
def check_api(next_page, retry_count=0):
    """
    Checks the ClinicalTrials.gov API for study data.
    Args:
        next_page (str): The token for the next page of results. Use 'START' for the first page and 'N/A' to indicate no more pages.
        retry_count (int, optional): The current retry attempt count. Defaults to 0.
    Returns:
        tuple: A tuple containing a boolean indicating success or failure, and the response object if successful, or None if not.
    """
    logging.info("API...")
    global response
    BASE_URL = "https://clinicaltrials.gov/api/v2/studies?sort=LastUpdatePostDate&pageSize=1000"

    if next_page == 'START':
        URL = BASE_URL
    elif next_page == 'N/A':
        return False, None  
    else:
        URL = f"{BASE_URL}&pageToken={next_page}"

    try:
        response = requests.get(URL)
        response.raise_for_status()
        return True, response
    except requests.exceptions.HTTPError as err:
        logging.info(f"HTTP error: {err}")
        if retry_count < MAX_RETRIES:
            time.sleep(RETRY_DELAY)
            return check_api(next_page, retry_count + 1)
        else:
            return False, None
    except requests.exceptions.RequestException as err:
        if retry_count < MAX_RETRIES:
            time.sleep(RETRY_DELAY)
            return check_api(next_page, retry_count + 1)
        else:
            return False, None

def get_data(response):
    """
    Extracts and processes clinical trial data from a given API response.
    Args:
        response (requests.Response): The response object from the API call containing clinical trial data in JSON format.
    Returns:
        tuple: A tuple containing:
            - next_page (str): The token for the next page of results, or 'N/A' if not available.
            - num_studies (int): The number of studies processed from the response.
    The function processes each study in the response to extract relevant information, including:
        - NCT ID
        - URL
        - Study Type
        - Official Title
        - Brief Title
        - Status
        - Start Date
        - Completion Date
        - Phase
        - Sponsor
        - Location
        - City
        - Organization Class
        - Keywords
        - Brief Summary
        - Detailed Summary
        - Intervention Name
        - Intervention Type
        - Intervention Description
        - Gender
        - Minimum Age
        - Maximum Age
        - Conditions
        - Enrollment
        - Inclusion Criteria
        - Exclusion Criteria
        - Healthy Volunteers
    The extracted data is stored in a dictionary for each study and added to a list of processed data.
    """
    # Extract studies from the response
    data = response.json()
    studies = data['studies']
    next_page = data.get('nextPageToken', 'N/A')

    # Iterate through each study and extract relevant information
    for study in studies:
        nct_id = study.get('protocolSection', {}).get('identificationModule', {}).get('nctId', 'N/A')
        study_url = f"https://clinicaltrials.gov/study/{nct_id}"
        eligibility_criteria = study.get('protocolSection', {}).get('eligibilityModule', {}).get('eligibilityCriteria', 'N/A')

        # Separate inclusion and exclusion criteria
        if "Inclusion Criteria:" in eligibility_criteria and "Exclusion Criteria:" in eligibility_criteria:
            split_criteria = eligibility_criteria.split("Exclusion Criteria:")
            inclusion_criteria = split_criteria[0].replace("Inclusion Criteria:", "").strip()
            exclusion_criteria = split_criteria[1].strip() if len(split_criteria) > 1 else "N/A"
        else:
            inclusion_criteria = "N/A"
            exclusion_criteria = "N/A"

        study_data = {
            # Study data
            'NCT ID': nct_id,
            'URL': study_url,
            'Study Type': study.get('protocolSection', {}).get('designModule', {}).get('studyType', 'N/A'),
            'Official_title': study.get('protocolSection', {}).get('identificationModule', {}).get('officialTitle', 'N/A'),
            'Title': study.get('protocolSection', {}).get('identificationModule', {}).get('briefTitle', 'N/A'),
            'Status': study.get('protocolSection', {}).get('statusModule', {}).get('overallStatus', 'N/A'),
            'Start Date': study.get('protocolSection', {}).get('statusModule', {}).get('startDateStruct', {}).get('date', 'N/A'),
            'Completion Date': study.get('protocolSection', {}).get('statusModule', {}).get('completionDateStruct', {}).get('date', 'N/A'),
            'Phase': study.get('protocolSection', {}).get('designModule', {}).get('phases', ['N/A']),
            'Sponsor': study.get('protocolSection', {}).get('sponsorCollaboratorsModule', {}).get('leadSponsor', {}).get('name', 'N/A'),
            'Location': 'N/A',
            'City': 'N/A',
            'Organization Class': study.get('protocolSection', {}).get('identificationModule', {}).get('organization', {}).get('class', 'N/A'),
            'Keywords': ', '.join(study.get('protocolSection', {}).get('conditionsModule', {}).get('keywords', ['N/A'])),
            'Brief Summary': study.get('protocolSection', {}).get('descriptionModule', {}).get('briefSummary', 'N/A'),
            'Detailed_summary': study.get('protocolSection', {}).get('descriptionModule', {}).get('detailedDescription', 'N/A'),
            # Intervention
            'Intervention Name': 'N/A',
            'Intervention Type': 'N/A',
            'Intervention Description': 'N/A',
            # Participants
            'Gender': study.get('protocolSection', {}).get('eligibilityModule', {}).get('sex', 'N/A'),
            'Minimum Age': study.get('protocolSection', {}).get('eligibilityModule', {}).get('minimumAge', 'N/A'),
            'Maximum Age': study.get('protocolSection', {}).get('eligibilityModule', {}).get('maximumAge', 'N/A'),
            'Conditions': ', '.join(study.get('protocolSection', {}).get('conditionsModule', {}).get('conditions', ['N/A'])),
            'Enrollment': study.get('protocolSection', {}).get('designModule', {}).get('enrollmentInfo', {}).get('count', 'N/A'),
            'Inclusion Criteria': inclusion_criteria,
            'Exclusion Criteria': exclusion_criteria,
            'Healthy Volunteers': study.get('protocolSection', {}).get('eligibilityModule', {}).get('healthyVolunteers', 'N/A')
        }
        
        # Update 'Location' and 'City' if available
        location = study.get('protocolSection', {}).get('contactsLocationsModule', {}).get('locations', [])
        if location:
            study_data['Location'] = location[0].get('country', 'N/A')
            study_data['City'] = location[0].get('city', 'N/A')
        # Update 'Intervention' if available
        intervention = study.get('protocolSection', {}).get('armsInterventionsModule', {}).get('interventions', {})
        if intervention:
            study_data['Intervention Name'] = intervention[0].get('name', 'N/A')
            study_data['Intervention Type'] = intervention[0].get('type', 'N/A')
            study_data['Intervention Description'] = intervention[0].get('description', 'N/A')

        # Add the extracted information to the list
        processed_data.append(study_data)
    return next_page, len(studies)

def extract_age_and_unit(age_str):
    """
    Extracts the numerical age and its unit from a given age string.

    Args:
        age_str (str): A string containing the age and its unit (e.g., '25 Years', '3 Months').

    Returns:
        tuple: A tuple containing the numerical age (int) and the unit (str). 
               If the age string is NaN or does not match the expected format, 
               returns (None, 'N/A').
    """
    if pd.isna(age_str):
        return None, 'N/A'
    match = re.search(r'(\d+)\s*(Years|Months|Days|Hours)?', age_str, re.IGNORECASE)
    if match:
        value = int(match.group(1))
        unit = match.group(2) if match.group(2) else 'N/A'
        return value, unit.capitalize()
    return None, 'N/A'    

def homogenize_sponsor(sponsor_name):
    """
    Standardizes the sponsor name by checking and replacing specific names.

    This function checks if the given sponsor name contains the specific name 
    'Merck Sharp & Dohme' (case insensitive) and replaces it with 'MSD'. 
    If not, it iterates through a predefined list of sponsors (SPONSORS) and 
    returns the first match found (case insensitive). If no match is found, 
    it returns the original sponsor name.

    Args:
        sponsor_name (str): The name of the sponsor to be homogenized.

    Returns:
        str: The standardized sponsor name.
    """
    # Check and replace the specific name 'Merck Sharp & Dohme' with 'MSD'
    if 'merck sharp & dohme' in sponsor_name.lower():
        return 'MSD'
    for sponsor in SPONSORS:
        if sponsor.lower() in sponsor_name.lower():
            return sponsor
    return sponsor_name

def classify_by_keywords(row, keyword_dict, columns):
    """
    Classifies a row based on the presence of keywords in specified columns.
    This function takes a row from a DataFrame, a dictionary of keywords, and a list of column names.
    It combines the content of the specified columns into a single lowercase string and checks for the presence
    of any keywords from the dictionary. If a match is found, it returns the corresponding group name.
    If no match is found, it returns 'NO' if the keyword dictionary is not equal to condition_keywords, otherwise 'OTHER'.
    Args:
        row (pd.Series): A row from a DataFrame.
        keyword_dict (dict): A dictionary where keys are group names and values are lists of keywords.
        columns (list): A list of column names to be combined and searched for keywords.
    Returns:
        str: The group name if a keyword match is found, otherwise 'NO' or 'OTHER'.
    """
    # Combine the content of relevant columns into a string and convert it to lowercase
    text_lower = ' '.join([str(row[col]).lower() for col in columns if pd.notna(row[col])])

    # Search for matches with the keywords
    for group, keywords in keyword_dict.items():
        if any(keyword in text_lower for keyword in keywords):
            return group
    return 'NO' if keyword_dict != condition_keywords else 'OTHER' # type: ignore

def preprocess(df):
    """
    Preprocesses the given DataFrame by performing various data cleaning and transformation tasks.
    Parameters:
    df (pd.DataFrame): The input DataFrame containing clinical trial data.
    Returns:
    pd.DataFrame: The preprocessed DataFrame with cleaned and transformed data.
    The preprocessing steps include:
    - Converting 'Start Date' and 'Completion Date' columns to datetime format.
    - Standardizing the 'Gender' column values.
    - Extracting age values and units from 'Minimum Age' and 'Maximum Age' columns.
    - Simplifying the 'Phase' column to contain only the first character.
    - Filling missing values in 'Location' and 'City' columns with 'N/A'.
    - Converting 'Conditions' column values to lowercase.
    - Classifying rows based on keywords into various categories such as 'Condition Grouped', 'Genetic', 'Advanced Therapies', 'Cancer', 'Enfermedades Raras', and 'Diabetes'.
    """
    # DATE
    df['Start Date'] = pd.to_datetime(df['Start Date'], errors='coerce')
    df['Completion Date'] = pd.to_datetime(df['Completion Date'], errors='coerce')

    # GENDER
    df['Gender'] = df['Gender'].replace({'ALL': 'All', 'M': 'Male', 'F': 'Female'})

    # AGE
    df['Minimum Age Value'], df['Minimum Age Unit'] = zip(*df['Minimum Age'].apply(extract_age_and_unit))
    df['Maximum Age Value'], df['Maximum Age Unit'] = zip(*df['Maximum Age'].apply(extract_age_and_unit))

    # PHASE
    df['Phase'] = df['Phase'].apply(lambda x: x[0] if x else None)

    # LOCATION
    df['Location'] = df['Location'].fillna('N/A')
    df['City'] = df['City'].fillna('N/A')

    # CONDITIONS
    df['Conditions'] = df['Conditions'].str.lower()

    # Classification
    columns_to_classify = ['Conditions', 'Official_title', 'Title', 'Brief Summary', 'Detailed_summary', 'Keywords', 'Inclusion Criteria', 'Intervention Name', 'Intervention Description']
    
    try:
        df.loc[:, 'Condition Grouped'] = df.apply(classify_by_keywords, axis=1, keyword_dict=condition_keywords, columns=columns_to_classify) # type: ignore
        df.loc[:, 'Genetic'] = df.apply(classify_by_keywords, axis=1, keyword_dict=genetic_keywords, columns=columns_to_classify) # type: ignore
        df.loc[:, 'Advanced Therapies'] = df.apply(classify_by_keywords, axis=1, keyword_dict=advanced_therapies_keywords, columns=columns_to_classify) # type: ignore
        df.loc[:, 'Cancer'] = df.apply(classify_by_keywords, axis=1, keyword_dict=cancer_keywords, columns=columns_to_classify) # type: ignore
        df.loc[:, 'Enfermedades Raras'] = df.apply(classify_by_keywords, axis=1, keyword_dict=rare_diseases_keywords, columns=columns_to_classify) # type: ignore
        df.loc[:, 'Diabetes'] = df.apply(classify_by_keywords, axis=1, keyword_dict=diabetes_keywords, columns=columns_to_classify) # type: ignore
    except Exception as e:
        logging.error(f"Error classifying keywords: {e}")
    
    return df

def csv_save(df, df_name):
    """
    Save the given DataFrame to an Excel file.
    Parameters:
    df (pandas.DataFrame): The DataFrame to be saved.
    df_name (str): The name of the Excel file (without extension).
    Returns:
    None
    """
    # Save the cleaned DataFrame to a new Excel file
    df.to_csv(f"{df_name}.csv", index=False)

    # Print confirmation message
    print(f"The data has been successfully saved as {df_name}.csv")

def main():  
    """
    Main function to fetch and process clinical trial data.
    This function performs the following steps:
    1. Initializes the state and total_records variables.
    2. Enters a loop to fetch data from an API while the state is True and the total number of records is less than MAX_RECORDS.
    3. Calls check_api to determine if there are more pages to process.
    4. If more pages are available, calls get_data to fetch the next page of data and updates the total_records count.
    5. Breaks the loop if there are no more pages or if the maximum number of records is reached.
    6. Converts the processed data into a pandas DataFrame.
    7. Preprocesses the DataFrame and saves it to an Excel file named "clinical_trials_cleaned_all_sponsors".
    8. Homogenizes the 'Sponsor' column in the DataFrame.
    9. Filters the DataFrame to include only specified sponsors.
    10. Saves the filtered DataFrame to an Excel file named "clinical_trials_cleaned".
    Global Variables:
    - nextPage: Tracks the next page to fetch from the API.
    - processed_data: Stores the data fetched from the API.
    Note:
    - MAX_RECORDS: The maximum number of records to fetch.
    - SPONSORS: A list of sponsors to filter the data.
    Returns:
    None
    """
    global nextPage, processed_data
    state = True
    total_records = 0

    # The loop continues while the state is True and the maximum number of records has not been reached
    while state and total_records < MAX_RECORDS:
        state, _ = check_api(nextPage)
        if state:
            nextPage, records_fetched = get_data(response)
            total_records += records_fetched
            # If nextPage is 'N/A', it means there are no more pages to process
            if nextPage == 'N/A':
                print("There are no more pages to process.")
                break
            if total_records >= MAX_RECORDS:
                # If the maximum number of records is reached, adjust processed_data to the desired size
                processed_data = processed_data[:MAX_RECORDS]
                print("The maximum number of records has been reached.")
                break

    df = pd.DataFrame(processed_data)
    df = preprocess(df)
    
    csv_save(df, "clinical_trials_cleaned_all_sponsors")

    # SPONSORS HOMOGENIZATION
    df['Sponsor'] = df['Sponsor'].apply(homogenize_sponsor)
    df = df[df['Sponsor'].isin(SPONSORS)]

    csv_save(df, "clinical_trials_sponsorFiltered")