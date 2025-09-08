# Podcast Theme Analyzer Architecture

## Core Architecture Overview

The system is composed of several small, independent services that communicate through a central database and a message queue. This architecture prevents a failure in one part (e.g., the transcription service is down) from bringing down the entire system.

Here's a high-level flow:

```
Ingestor -> Job Queue -> Processor -> Database <- Analyzer (LLM) -> Database
```

## File & Folder Structure

A clean, organized project structure is key. This layout separates configuration, source code, and data.

```bash
podcast-theme-analyzer/
│
├── .env                  # Stores secrets and environment variables (API keys, DB path)
├── config.yaml           # Configuration for podcasts, LLM prompts, etc.
├── requirements.txt      # Python package dependencies
├── README.md             # Project documentation
│
├── data/                 # All persistent data lives here
│   ├── raw_audio/        # Optional: for storing original audio files
│   ├── transcripts.db    # SQLite database for structured data
│
└── src/                  # Main application source code
    ├── __init__.py
    ├── ingestor.py       # Service to find new podcast episodes
    ├── processor.py      # Service to download, transcribe, and clean data
    ├── analyzer.py       # Service to connect to an LLM and find themes
    │
    └── common/           # Shared utilities
        ├── __init__.py
        ├── database.py   # Database connection and schema setup
        └── llm_client.py # Wrapper for communicating with the LLM API
```

## Component Breakdown

Each Python script in the `src/` directory acts as a microservice. You can run them as separate processes.

### 1\. Ingestor (`src/ingestor.py`)

  * **What it does:** Its only job is to discover new podcast episodes that need to be processed.
  * **How it works:** It reads a list of podcast RSS feeds from `config.yaml`. It then checks the database to see which episodes from those feeds have already been processed. For any new, unprocessed episodes, it creates a "job" and places it onto a message queue (like Redis or RabbitMQ).
  * **Example Job Message:** `{'episode_url': 'https://example.com/podcast.mp3', 'rss_feed_url': '...'}`

### 2\. Processor (`src/processor.py`)

  * **What it does:** This is the workhorse that gets the transcript. It listens to the message queue for new jobs from the `Ingestor`.
  * **How it works:**
    1.  Pulls a job from the queue.
    2.  Downloads the audio file from the `episode_url` or finds a pre-existing transcript in the RSS feed data.
    3.  If only audio is available, it sends the file to a transcription service (e.g., OpenAI's Whisper, AssemblyAI).
    4.  It performs basic text cleaning (e.g., removing timestamps, standardizing speaker names).
    5.  It saves the cleaned transcript and all relevant metadata (episode title, date, URL) into the database. It also marks the episode with a status like `PROCESSED`.

### 3\. Analyzer (`src/analyzer.py`)

  * **What it does:** This component connects to the LLM to perform the actual theme identification.
  * **How it works:**
    1.  It queries the database for episodes with the status `PROCESSED` that do not yet have themes.
    2.  For each episode, it takes the full transcript.
    3.  It wraps the transcript in a carefully crafted prompt (which you can store in `config.yaml`). The prompt will instruct the LLM to return 5-10 recurring themes in a specific format (e.g., JSON).
    4.  It sends the request to the LLM via the `llm_client.py` wrapper.
    5.  It parses the LLM's response and saves the identified themes back into the database, linked to the correct episode. It then updates the episode's status to `ANALYZED`.

### 4\. Common Utilities (`src/common/`)

  * **`database.py`**: Handles all interactions with the SQLite database. It will contain functions to initialize the tables (`CREATE TABLE ...`), insert new episodes, update statuses, and query for data. This centralizes your data logic.
  * **`llm_client.py`**: A simple client for whatever LLM you choose (e.g., OpenAI, Anthropic, Gemini). It handles API key authentication (read from `.env`), request formatting, and retries.

## State & Connectivity

This architecture cleanly separates different types of state.

### Transient State (The Job Queue)

**The message queue** (Redis is a great, simple choice) holds the list of "work to be done." This state is temporary. Once a job is picked up and completed by the `Processor`, it's gone from the queue. This decouples the `Ingestor` from the `Processor`. The `Ingestor` doesn't need to know or care if the `Processor` is running; it just adds jobs to the queue.

### Persistent State (The Database & File System)

This is your **single source of truth**.

  * **The SQLite Database (`transcripts.db`)** stores all structured, queryable data:
      * A `podcasts` table (podcast name, rss\_feed\_url).
      * An `episodes` table (episode\_title, url, publish\_date, status, transcript, foreign key to `podcasts`).
      * A `themes` table (theme\_description, foreign key to `episodes`).
  * **The File System (`data/raw_audio/`)** is for unstructured blobs like the original MP3 files, which are expensive to store in a database. The database simply stores a *path* to the file.

### How Services Connect

1.  **Ingestor -\> Queue:** The `Ingestor` writes a JSON message to a Redis list (acting as a queue).
2.  **Queue -\> Processor:** The `Processor` runs in a loop, listening for new messages on that Redis list.
3.  **Processor -\> Database:** After processing, the `Processor` writes the transcript and metadata to the SQLite database.
4.  **Analyzer \<-\> Database:** The `Analyzer` is independent. It periodically polls the database, looking for rows it can work on. It reads a transcript *from* the database and writes the resulting themes *back to* the database.
