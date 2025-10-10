Airflow AI/GenAI Project Repository

Overview

This repository serves as a Project Playground for developing, scheduling, and orchestrating data and AI/ML pipelines using Apache Airflow.

The primary purpose of the DAGs in this collection is to demonstrate the secure and scalable integration of Generative AI (GenAI), specifically the Google Gemini API, into production-ready data workflows.

Repository Structure & Core Features

This repository is structured to support multiple Airflow DAGs that share common dependencies and security configurations.

Directory Structure

dags/: Contains all the individual Directed Acyclic Graph (.py) files. Each file represents an automated, scheduled pipeline.

.env: (Local use only) Stores sensitive environment variables, such as API keys. This file is excluded by .gitignore.

docker-compose.yaml: Defines the local development Airflow environment (scheduler, worker, webserver).

Example DAG Features

DAGs within this repository typically focus on scheduled reports and dynamic content generation. For example, the daily_motivation_playlist.py DAG demonstrates:

Contextual Content: Generating output (e.g., music or coffee suggestions) based on the current date and season.

Structured Output: Utilizing JSON schemas in API calls to ensure predictable, machine-readable results.

Secure API Calls: Implementing robust security and retry mechanisms for external service calls.

Architecture & How It Works

All DAGs in this repository share a common dependency on the Gemini API for dynamic generation.

1. Secure API Key Management

The most critical architectural principle is security. We avoid hardcoding secrets by following this process:

Secret Storage: The key is stored in the local, untracked .env file: GEMINI_API_KEY="YOUR_API_KEY_GOES_HERE".

Environment Integration: The docker-compose.yaml file is configured to load this variable and pass it to the necessary Airflow services (scheduler and worker) via the YAML syntax: GEMINI_API_KEY: ${GEMINI_API_KEY}.

Code Access: Python code within the DAGs accesses the key securely using the standard os.getenv("GEMINI_API_KEY") method.

2. Robust API Interaction

Model: All GenAI tasks currently target the gemini-2.5-flash-preview-05-20 endpoint.

Reliability: All API-calling functions include an exponential backoff retry mechanism to handle transient network issues and ensure DAG stability.

Data Flow (XComs): Results from generator tasks are pushed via XComs (Cross-Communication) to a final logging or reporting task.

Setup and Prerequisites

1. Dependencies

This project requires Python packages for API interaction and handling.

# Required Python packages
pip install requests


2. Environment Setup (Crucial)

Follow these steps to set up the environment and securely configure the API key for all DAGs in this repository:

Obtain Key: Get your valid Gemini API key from Google AI Studio.

Create .env: Create a file named .env in the root of this repository and add your key:

# .env
GEMINI_API_KEY="PASTE_YOUR_KEY_HERE"


Configure YAML: Ensure the docker-compose.yaml file includes the GEMINI_API_KEY reference in the environment section of the airflow-scheduler and airflow-worker services (this should already be done if you followed previous steps).

Initial Build & Restart: Build and restart your Airflow containers to load the environment variables:

docker compose up --build --detach


3. Deployment

Place any new DAG file (e.g., new_report.py) into the dags/ folder. Airflow will automatically detect and load the pipeline within minutes.