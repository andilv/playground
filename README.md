# Gibberish Detection API

This project is a Python FastAPI application that provides an API endpoint to detect if a given text string is gibberish. It performs two levels of analysis:
1.  **Character-level gibberish**: Identifies if the text consists of random characters, is heavily misspelled, or uses letter combinations uncommon in English.
2.  **Syntactic gibberish**: Identifies if the text, even if composed of recognizable English words, appears to be a random, nonsensical ordering of those words.

The language profiles for both detection levels are generated using NLTK (Natural Language Toolkit) corpora.

## Features

-   POST endpoint `/detect` accepts a JSON payload with a `text` field.
-   Returns a JSON response indicating:
    -   `is_char_gibberish` (boolean) and `char_gibberish_confidence` (float 0-1) for character-level analysis.
    -   `is_syntactically_gibberish` (boolean) and `syntactic_confidence_score` (float 0-1) for word order analysis.
-   Language profiles dynamically generated using NLTK:
    -   Character trigrams from the 'words' corpus (top 3,000).
    -   Word bigrams from the 'brown' corpus (top 60,000).
-   Input validation.
-   Comprehensive unit tests.

## Project Structure

```
.
├── gibberish_detector.py # Core logic for gibberish detection (character & syntactic)
├── main.py               # FastAPI application, endpoint definition (version 0.2.0)
├── requirements.txt      # Project dependencies
├── tests/                # Unit tests
│   ├── __init__.py
│   └── test_main.py
└── README.md             # This file
```

## Prerequisites - NLTK Data

This application uses several NLTK resources. The script attempts to download these automatically if they are not found. However, in some environments, manual download might be necessary.

Required NLTK resources for profile generation:
-   `words`: For the character-level gibberish model.
-   `brown`: For building the word bigram model for syntactic analysis.

You can download these resources by running the following Python command:
```bash
python -m nltk.downloader words brown
```
Or within a Python interpreter:
```python
import nltk
nltk.download('words')
nltk.download('brown')
```
(The word tokenization for syntactic analysis uses `TreebankWordTokenizer`, which is part of the standard NLTK library and does not require a separate data download like 'punkt'.)

## Setup and Installation

1.  **Clone the repository (if applicable).**
2.  **Create a virtual environment (recommended).**
    ```bash
    python -m venv venv
    source venv/bin/activate  # On Windows use `venv\Scripts\activate`
    ```
3.  **Install dependencies:**
    ```bash
    pip install -r requirements.txt
    ```
4.  **Ensure NLTK resources are available** (see "Prerequisites" section).

## Running the Application

1.  **Start the FastAPI server using Uvicorn:**
    ```bash
    uvicorn main:app --reload
    ```
    The first time you run this, `gibberish_detector.py` will generate language profiles from NLTK, which may take some time and might attempt to download missing NLTK data.

2.  **Access the API documentation:**
    Open your browser and go to [http://127.0.0.1:8000/docs](http://127.0.0.1:8000/docs) (Swagger UI) or [http://127.0.0.1:8000/redoc](http://127.0.0.1:8000/redoc) (ReDoc).

## Running Tests

1.  **Ensure all dependencies and NLTK resources are installed/downloaded.**
2.  **Run tests using pytest:**
    ```bash
    pytest -v
    ```

## API Endpoint: `/detect`

-   **Method:** `POST`
-   **Request Body:**
    ```json
    {
        "text": "your text to analyze here"
    }
    ```
-   **Success Response (200 OK):**
    ```json
    {
        "is_char_gibberish": false,
        "char_gibberish_confidence": 0.95,
        "is_syntactically_gibberish": true,
        "syntactic_confidence_score": 0.88
    }
    ```
    -   `is_char_gibberish` (boolean): `true` if text seems like random characters or heavily misspelled.
    -   `char_gibberish_confidence` (float): Confidence in the character-level assessment.
    -   `is_syntactically_gibberish` (boolean): `true` if the sequence of (otherwise valid) words seems random or nonsensical.
    -   `syntactic_confidence_score` (float): Confidence in the syntactic assessment.

-   **Error Responses:** `400 Bad Request`, `422 Unprocessable Entity`.

## How Gibberish Detection Works

The API employs a two-stage detection process:

1.  **Character-Level Gibberish Detection:**
    -   **Profile**: A set of the top 3,000 most common character trigrams derived from the NLTK 'words' corpus.
    -   **Logic**: The input text is processed to extract its character trigrams. If the percentage of these trigrams *not* found in the profile exceeds a threshold (currently 0.7), the text is flagged as character-level gibberish.

2.  **Syntactic Gibberish (Random Word Order) Detection:**
    -   **Profile**: A set of the top 60,000 most common word bigrams (pairs of adjacent words) derived from the NLTK 'brown' corpus (a large collection of varied American English texts).
    -   **Logic**: The input text is tokenized into words using NLTK's `TreebankWordTokenizer`. Word bigrams are then formed from these words. If the percentage of these input bigrams *not* found in the profile exceeds a threshold (currently 0.80), the text is flagged as syntactically gibberish.

**Note on Language Specificity**: Both detection mechanisms are currently profiled for **English**. Their accuracy will be lower for other languages. Text in other languages might be flagged as gibberish by one or both detectors.
The syntactic model is based on typical word sequences in the Brown corpus; highly novel or poetic English constructions, or very common colloquial phrases not well-represented in the Brown corpus, might also be flagged if their word bigrams are rare.
EOL
