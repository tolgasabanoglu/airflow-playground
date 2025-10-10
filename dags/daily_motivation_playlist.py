from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import logging
import random
import time
import json
import requests 
import os # Crucial for fetching the API key securely

log = logging.getLogger(__name__)

# --- API Constants (For Gemini Generation) ---
GEMINI_API_URL = "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash-preview-05-20:generateContent"
API_KEY = os.getenv("GEMINI_API_KEY", "") 

# --- Helper Function: Determines the Season ---
def get_current_season():
    """
    Determines the current season based on the month (Northern Hemisphere).
    """
    now = datetime.now()
    month = now.month
    
    if month in (12, 1, 2):
        return 'Winter'
    elif month in (3, 4, 5):
        return 'Spring'
    elif month in (6, 7, 8):
        return 'Summer'
    else: # 9, 10, 11
        return 'Autumn'

# --- 1. Motivation Logic Function (DYNAMIC GENERATION) ---
def generate_motivation(**kwargs):
    """
    Generates a unique, daily motivational message using the Gemini API.
    """
    log.info("Generating unique daily motivational message using Gemini API...")
    
    # Define a clear failure message for motivation
    failure_message = "Gemini API failed to generate motivation. Please check logs and API Key."

    if not API_KEY:
        log.warning("API_KEY not found in environment. Using failure message.")
        return failure_message

    system_prompt = "You are a concise, upbeat motivational coach. Generate one short, unique, and inspiring sentence for the start of the day. Do not include quotes or names."
    user_query = "Generate a new motivational sentence about starting strong and focusing on daily goals."

    payload = {
        "contents": [{"parts": [{"text": user_query}]}],
        "systemInstruction": {"parts": [{"text": system_prompt}]}
    }

    # --- API Call with Exponential Backoff ---
    max_retries = 3
    delay = 1
    
    for attempt in range(max_retries):
        try:
            # Make the POST request to the Gemini API endpoint
            response = requests.post(f"{GEMINI_API_URL}?key={API_KEY}", json=payload, timeout=10)
            response.raise_for_status() 
            
            result = response.json()
            
            # Extract the generated text from the structured response
            motivation_message = result.get('candidates', [{}])[0].get('content', {}).get('parts', [{}])[0].get('text', None)
            
            if motivation_message:
                log.info(f"Gemini-Generated Motivation: {motivation_message.strip()}")
                return motivation_message.strip()
            else:
                log.error("Gemini API returned no text content.")

        except requests.exceptions.RequestException as e:
            log.warning(f"Gemini API call failed on attempt {attempt + 1}/{max_retries}: {e}")
            if attempt < max_retries - 1:
                time.sleep(delay)
                delay *= 2 
            else:
                log.error("Gemini API call failed after max retries.")
                break

    # --- Fallback to Failure Message ---
    log.error(f"Using failure message: {failure_message}")
    return failure_message

# --- 2. Playlist Suggestion Logic Function (DYNAMIC ALBUM/ARTIST) ---
def suggest_playlist(**kwargs):
    """
    Determines the current season and suggests a specific Artist, Album, and Reason 
    using the Gemini API with a structured JSON response.
    """
    season = get_current_season()
    log.info(f"It is currently {season}. Generating dynamic album/artist suggestion.")
    
    # Define prompts based on season
    seasonal_prompts = {
        'Winter': "Suggest a reflective, cozy, and slightly melancholic album, fitting for the deep winter months.",
        'Spring': "Suggest an upbeat, optimistic, and energetic album suitable for fresh starts and new growth.",
        'Summer': "Suggest a sunny, driving, classic album with a clear, energetic sound for summer road trips or outdoor chilling.",
        'Autumn': "Suggest a complex, warm, contemplative jazz or instrumental album perfect for crisp air and contemplation.",
    }
    
    user_query = seasonal_prompts[season]
    
    # Define a structured JSON failure message
    failure_suggestion = {
        "artist": "API Failure", 
        "album": "Check Logs", 
        "reason": f"Music generation failed for {season}. Check the worker logs for API errors.",
    }
    failure_json_str = json.dumps(failure_suggestion)
    
    # Check for API Key before making the call
    if not API_KEY:
        log.warning("API_KEY not found in environment. Using failure message for music suggestion.")
        return failure_json_str
    
    # Define the required JSON output schema for the model
    schema = {
        "type": "OBJECT",
        "properties": {
            "artist": {"type": "STRING", "description": "The name of the recommended artist."},
            "album": {"type": "STRING", "description": "The name of the recommended album."},
            "reason": {"type": "STRING", "description": "A short, one-sentence reason for the recommendation."},
        },
        "required": ["artist", "album", "reason"]
    }

    system_prompt = f"You are a music curator specializing in seasonal mood playlists. Based on the {season} season, recommend a single album. Your output MUST be a valid JSON object matching the provided schema."

    payload = {
        "contents": [{"parts": [{"text": user_query}]}],
        "systemInstruction": {"parts": [{"text": system_prompt}]},
        "generationConfig": {
            "responseMimeType": "application/json",
            "responseSchema": schema
        }
    }
    
    # --- API Call with Exponential Backoff ---
    max_retries = 3
    delay = 1
    
    for attempt in range(max_retries):
        try:
            response = requests.post(f"{GEMINI_API_URL}?key={API_KEY}", json=payload, timeout=15)
            response.raise_for_status() 
            
            result = response.json()
            
            # Extract and parse the JSON string from the response
            json_text = result.get('candidates', [{}])[0].get('content', {}).get('parts', [{}])[0].get('text', '{}')
            suggestion = json.loads(json_text)
            
            if 'artist' in suggestion and 'album' in suggestion:
                log.info(f"Gemini-Generated Suggestion: Artist={suggestion['artist']}, Album={suggestion['album']}")
                # Return the structured data as a JSON string for easy XCom transfer
                return json_text 
            else:
                log.error(f"Gemini API returned invalid JSON structure: {json_text}.")

        except (requests.exceptions.RequestException, json.JSONDecodeError) as e:
            log.warning(f"API/JSON error on attempt {attempt + 1}/{max_retries}: {e}")
            if attempt < max_retries - 1:
                time.sleep(delay)
                delay *= 2 
            else:
                log.error("API call failed after max retries.")
                break

    # --- Fallback to Failure Message (No Static Suggestions) ---
    log.error("API call failed after max retries. Using failure message.")
    return failure_json_str

# --- 3. Coffee Suggestion Logic Function (DYNAMIC BEVERAGE) ---
def suggest_daily_coffee(**kwargs):
    """
    Generates a daily coffee/beverage suggestion based on the season or month, 
    using the Gemini API with structured JSON output.
    """
    now = datetime.now()
    month = now.month
    season = get_current_season()
    
    # --- Dynamic Prompt Logic ---
    user_query_base = f"Suggest a unique daily coffee or beverage based on the {season} season."
    current_vibe = season
    
    if month == 10: # October is great for pumpkin drinks
        user_query_base = "Suggest a delicious coffee or beverage, prioritizing pumpkin or heavily spiced autumnal flavors suitable for Halloween season."
        current_vibe = "Halloween Time Vibe"
    elif season == 'Summer':
        user_query_base = "Suggest a refreshing cold coffee or iced beverage, perfect for a hot summer day. Ensure the style is cold."
        current_vibe = f"{season} (Iced Focus)"

    log.info(f"Current Vibe: {current_vibe}. Generating coffee suggestion.")

    # Define a structured JSON failure message
    failure_suggestion = {
        "drink_name": "API Failure", 
        "style": "Check Logs", 
        "flavour_profile": f"Beverage generation failed for {current_vibe}. Check the worker logs for API errors.",
    }
    failure_json_str = json.dumps(failure_suggestion)
    
    if not API_KEY:
        log.warning("API_KEY not found in environment. Using failure message for coffee suggestion.")
        return failure_json_str

    # Define the required JSON output schema for the model
    schema = {
        "type": "OBJECT",
        "properties": {
            "drink_name": {"type": "STRING", "description": "The name of the recommended beverage (e.g., 'Maple Pecan Latte')."},
            "style": {"type": "STRING", "description": "The temperature/style (e.g., 'Hot', 'Iced', 'Blended')."},
            "flavour_profile": {"type": "STRING", "description": "A short, descriptive flavor profile (e.g., 'Sweet, nutty, and comforting')."},
        },
        "required": ["drink_name", "style", "flavour_profile"]
    }

    system_prompt = f"You are a professional barista and daily beverage curator. Based on the current vibe of '{current_vibe}', recommend a single unique coffee or beverage. Your output MUST be a valid JSON object matching the provided schema."

    payload = {
        "contents": [{"parts": [{"text": user_query_base}]}],
        "systemInstruction": {"parts": [{"text": system_prompt}]},
        "generationConfig": {
            "responseMimeType": "application/json",
            "responseSchema": schema
        }
    }
    
    # --- API Call with Exponential Backoff ---
    max_retries = 3
    delay = 1
    
    for attempt in range(max_retries):
        try:
            response = requests.post(f"{GEMINI_API_URL}?key={API_KEY}", json=payload, timeout=15)
            response.raise_for_status() 
            
            result = response.json()
            
            # Extract and parse the JSON string from the response
            json_text = result.get('candidates', [{}])[0].get('content', {}).get('parts', [{}])[0].get('text', '{}')
            suggestion = json.loads(json_text)
            
            if 'drink_name' in suggestion and 'style' in suggestion:
                log.info(f"Gemini-Generated Coffee: {suggestion['drink_name']} ({suggestion['style']})")
                # Return the structured data as a JSON string for easy XCom transfer
                return json_text 
            else:
                log.error(f"Gemini API returned invalid JSON structure: {json_text}.")

        except (requests.exceptions.RequestException, json.JSONDecodeError) as e:
            log.warning(f"API/JSON error on attempt {attempt + 1}/{max_retries}: {e}")
            if attempt < max_retries - 1:
                time.sleep(delay)
                delay *= 2 
            else:
                log.error("API call failed after max retries.")
                break

    # --- Fallback to Failure Message ---
    log.error("API call failed after max retries. Using failure message.")
    return failure_json_str

# --- 4. Final Logging Function ---
def log_final_report(**kwargs):
    """
    Pulls motivation, playlist, and coffee reports, parses the JSON data, 
    and prints them clearly.
    """
    ti = kwargs['ti'] 
    
    # Pull data from previous tasks
    motivation_message = ti.xcom_pull(task_ids='generate_daily_motivation')
    playlist_json_str = ti.xcom_pull(task_ids='suggest_daily_playlist')
    coffee_json_str = ti.xcom_pull(task_ids='suggest_daily_coffee')
    
    current_season = get_current_season()
    
    # Safely parse the JSON strings
    try:
        playlist_data = json.loads(playlist_json_str)
    except (TypeError, json.JSONDecodeError):
        log.error("Failed to parse playlist data JSON.")
        playlist_data = None
        
    try:
        coffee_data = json.loads(coffee_json_str)
    except (TypeError, json.JSONDecodeError):
        log.error("Failed to parse coffee data JSON.")
        coffee_data = None
        
    log.info("====================================")
    log.info(f"====== YOUR DAILY MORNING REPORT ({current_season}) =====")
    log.info("====================================")
    
    # 1. MOTIVATION
    log.info(f"MOTIVATION:")
    log.info(f"-> {motivation_message}")
    log.info("")
    
    # 2. COFFEE SUGGESTION
    log.info("DAILY BEVERAGE SUGGESTION:")
    if coffee_data:
        log.info(f"Drink: {coffee_data.get('drink_name', 'N/A')}")
        log.info(f"Style: {coffee_data.get('style', 'N/A')}")
        log.info(f"Flavour: {coffee_data.get('flavour_profile', 'N/A')}")
    else:
        log.info(f"-> {coffee_json_str}")
    log.info("")

    # 3. MUSIC SUGGESTION
    log.info("MUSIC SUGGESTION:")
    if playlist_data:
        log.info(f"Album: {playlist_data.get('album', 'N/A')}")
        log.info(f"Artist: {playlist_data.get('artist', 'N/A')}")
        log.info(f"Reason: {playlist_data.get('reason', 'N/A')}")
    else:
        log.info(f"-> {playlist_json_str}")
        
    log.info("====================================")
    
    log.info("Final Report logged successfully.")


# --- DAG Definition ---
with DAG(
    dag_id='daily_motivation_and_playlist',
    start_date=datetime(2023, 1, 1),
    schedule_interval='0 7 * * *', # Runs every day at 7:00 AM UTC
    catchup=False,
    tags=['daily', 'motivation', 'music', 'coffee'],
) as dag:
    
    # Task 1: Generate a random motivational message (Gemini)
    motivation_task = PythonOperator(
        task_id='generate_daily_motivation',
        python_callable=generate_motivation,
    )

    # Task 2: Suggest a seasonal album/artist (Gemini with JSON)
    playlist_task = PythonOperator(
        task_id='suggest_daily_playlist',
        python_callable=suggest_playlist,
    )
    
    # Task 3: Suggest a seasonal coffee/beverage (Gemini with JSON)
    coffee_task = PythonOperator(
        task_id='suggest_daily_coffee',
        python_callable=suggest_daily_coffee,
    )

    # Task 4: Combine and Print Final Report
    final_report_task = PythonOperator(
        task_id='log_final_report',
        python_callable=log_final_report,
    )

    # Define the dependencies: all three generation tasks run in parallel, then the report task runs.
    [motivation_task, playlist_task, coffee_task] >> final_report_task
