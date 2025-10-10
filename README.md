Daily Personalized Morning Report Generator
Overview
This Apache Airflow Directed Acyclic Graph (DAG) is a scheduled data pipeline designed to generate a personalized, dynamic "Daily Morning Report." Instead of relying on static data, it leverages the power of the Google Gemini API to create contextually relevant and unique suggestions for motivation, music, and beverages, delivered directly to your Airflow logs every morning.

This project serves as a practical, real-world example of integrating modern Generative AI (GenAI) into a secure, scheduled data orchestration workflow.

Key Features
The DAG runs three parallel Python tasks that call the Gemini API, ensuring the suggestions are always fresh and tailored to the current date and season.

1. Daily Motivation
What it is: A concise, upbeat, and unique motivational sentence to start the day.

How it works: The model is given a strict system prompt to act as an upbeat coach, ensuring the output is always short and inspiring.

2. Seasonal Music Playlist Suggestion
What it is: A recommendation for a specific Artist and Album that matches the mood of the current season (Winter, Spring, Summer, Autumn).

How it works: The model is instructed to return a structured JSON object with the Artist, Album, and a short Reason, which is then parsed by the Python task.

3. Smart Daily Beverage Suggestion
What it is: A coffee or beverage recommendation tailored to the weather/time of year.

How it works: The Python logic dynamically adjusts the prompt sent to the Gemini API:

Summer: Specifically requests an iced or cold beverage.

October (Halloween Time): Prioritizes pumpkin-themed or heavily spiced autumnal drinks.

Other Seasons: Provides general, seasonal suggestions.

Architecture & How It Works
The entire pipeline is built within a single Airflow DAG file (daily_motivation_playlist.py) using standard Python components.

Orchestration (Airflow): The DAG is scheduled to run every day at a set time (e.g., 7:00 AM UTC).

API Security: The project relies on the GEMINI_API_KEY being securely passed to the Airflow worker/scheduler containers via environment variables (typically defined in a local .env file and configured in docker-compose.yaml).

Dynamic Generation (Gemini API): Three separate Python functions make POST requests to the Gemini API endpoint (gemini-2.5-flash-preview-05-20).

Requests use specific system prompts and JSON schemas to ensure predictable and reliable output formats.

An exponential backoff retry mechanism is implemented for each API call to gracefully handle transient network or service errors.

Data Flow (XComs): The three generation tasks push their resulting data (the motivation string and two JSON strings) into Airflow's XComs (Cross-Communication).

Final Report: The final task (log_final_report) pulls all three pieces of data from XComs, parses the JSON objects, and logs a clean, combined report to the Airflow task logs.

Setup and Prerequisites
1. Dependencies
This project requires a standard Airflow environment setup (usually via Docker Compose) with Python dependencies for network requests and JSON handling.

# Required Python packages
pip install requests

2. API Key Configuration (Crucial)
To use the dynamic generation features, you must configure your Gemini API Key.

Get Your Key: Obtain a valid API key from Google AI Studio.

Secure Storage (.env): Store your key securely in your Airflow environment's .env file (which should be added to .gitignore):

# .env
GEMINI_API_KEY="YOUR_API_KEY_GOES_HERE"

Airflow Integration (docker-compose.yaml): Ensure your Airflow services (scheduler and worker) are configured to load this key as an environment variable:

# Snippet from docker-compose.yaml
environment:
  # ... other airflow variables
  - GEMINI_API_KEY=${GEMINI_API_KEY}

3. Deployment
Place the daily_motivation_playlist.py file into your designated Airflow DAGs folder and restart your Airflow environment to load the pipeline.