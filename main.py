from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from typing import Optional

# Assuming gibberish_detector.py is in the same directory
from gibberish_detector import check_char_gibberish, check_random_word_order

app = FastAPI(
    title="Gibberish Detection API",
    description="Detects if a given text is gibberish at character-level (misspellings, random chars) and syntactic-level (random word order).",
    version="0.2.0" # Updated version
)

# Pydantic models for request and response
class DetectRequest(BaseModel):
    text: str = Field(..., min_length=1, description="The text to analyze for gibberish.")

class DetectResponse(BaseModel):
    is_char_gibberish: bool = Field(..., description="True if text is gibberish based on character patterns (e.g., random characters, misspellings).")
    char_gibberish_confidence: float = Field(..., ge=0.0, le=1.0, description="Confidence in the character-level gibberish assessment.")
    is_syntactically_gibberish: bool = Field(..., description="True if text appears to be a random ordering of recognizable words.")
    syntactic_confidence_score: float = Field(..., ge=0.0, le=1.0, description="Confidence in the syntactic gibberish assessment (random word order).")


@app.get("/")
async def root():
    return {"message": "Welcome to the Gibberish Detection API. Use the /detect endpoint to analyze text."}

@app.post("/detect", response_model=DetectResponse)
async def detect_gibberish_endpoint(request: DetectRequest):
    """
    Detects if the provided text is gibberish at character and syntactic levels.
    - **text**: The input string to analyze.
    """
    if not request.text: # Should be caught by Pydantic, but as a safeguard
        raise HTTPException(status_code=400, detail="Input text cannot be empty.")

    cleaned_text = request.text.strip()
    if not cleaned_text:
        raise HTTPException(status_code=400, detail="Input text cannot be empty after stripping whitespace.")

    # Character-level gibberish detection
    is_char_gib, char_conf = check_char_gibberish(cleaned_text)

    # Syntactic (random word order) gibberish detection
    is_synt_gib, synt_conf = check_random_word_order(cleaned_text)

    return DetectResponse(
        is_char_gibberish=is_char_gib,
        char_gibberish_confidence=char_conf,
        is_syntactically_gibberish=is_synt_gib,
        syntactic_confidence_score=synt_conf
    )
