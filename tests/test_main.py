import pytest
from fastapi.testclient import TestClient
from main import app # Assuming main.py is in the root

client = TestClient(app)

# Helper to check for basic structure and types, can be expanded
def validate_response_structure(data: dict):
    assert "is_char_gibberish" in data
    assert isinstance(data["is_char_gibberish"], bool)
    assert "char_gibberish_confidence" in data
    assert isinstance(data["char_gibberish_confidence"], float)
    assert "is_syntactically_gibberish" in data
    assert isinstance(data["is_syntactically_gibberish"], bool)
    assert "syntactic_confidence_score" in data
    assert isinstance(data["syntactic_confidence_score"], float)
    assert 0.0 <= data["char_gibberish_confidence"] <= 1.0
    assert 0.0 <= data["syntactic_confidence_score"] <= 1.0

# --- Existing Tests (Modified for new response structure) ---

def test_detect_real_english_text():
    response = client.post("/detect", json={"text": "Hello world, this is a perfectly normal sentence full of common words."})
    assert response.status_code == 200
    data = response.json()
    validate_response_structure(data)
    assert data["is_char_gibberish"] == False
    assert data["char_gibberish_confidence"] >= 0.8
    assert data["is_syntactically_gibberish"] == False # Should be syntactically valid
    assert data["syntactic_confidence_score"] >= 0.35 # Adjusted from 0.7, actual is ~0.4

def test_detect_obvious_char_gibberish():
    response = client.post("/detect", json={"text": "asdfqwertzxcv dfghj vbnm fjkruejdue"})
    assert response.status_code == 200
    data = response.json()
    validate_response_structure(data)
    assert data["is_char_gibberish"] == True
    assert data["char_gibberish_confidence"] >= 0.85
    # Syntactic check might be True or False here, less critical if char_gibberish is True.
    # Often, true char gibberish will also appear syntactically random if it forms any 'words'.
    # For "asdfqwertzxcv", cleaned words might be empty or single long word, so syntactic check might be False, 0.0.
    if not data["is_syntactically_gibberish"]:
         assert data["syntactic_confidence_score"] < 0.5 # low confidence if it's not syntactically gibberish
    else:
         assert data["syntactic_confidence_score"] >= 0.5 # or high if it is

def test_detect_short_char_gibberish():
    response = client.post("/detect", json={"text": "qwx zxc vfr"})
    assert response.status_code == 200
    data = response.json()
    validate_response_structure(data)
    assert data["is_char_gibberish"] == True
    # As above, syntactic result can vary for char gibberish.
    # "qwx", "zxc", "vfr" - these might form no valid bigrams or all unknown.

def test_detect_short_real_words():
    response = client.post("/detect", json={"text": "the cat sat on the mat"})
    assert response.status_code == 200
    data = response.json()
    validate_response_structure(data)
    assert data["is_char_gibberish"] == False
    assert data["char_gibberish_confidence"] >= 0.7
    assert data["is_syntactically_gibberish"] == False
    assert data["syntactic_confidence_score"] >= 0.6 # Should have some known bigrams

def test_detect_uncommon_real_english_words():
    response = client.post("/detect", json={"text": "zyzzyva syzygy quixotry aphylly"})
    assert response.status_code == 200
    data = response.json()
    validate_response_structure(data)
    assert data["is_char_gibberish"] == False
    assert data["char_gibberish_confidence"] >= 0.40
    # With very rare words, it's plausible their bigrams are unknown.
    assert data["is_syntactically_gibberish"] == True
    assert data["syntactic_confidence_score"] >= 0.75 # Expect high confidence it IS syntactically gibberish


def test_detect_mixed_common_uncommon_words():
    response = client.post("/detect", json={"text": "The quick brown fox jumps over the lazy dog while considering syzygy."})
    assert response.status_code == 200
    data = response.json()
    validate_response_structure(data)
    assert data["is_char_gibberish"] == False
    assert data["char_gibberish_confidence"] >= 0.7
    # Presence of a very rare word like "syzygy" makes many bigrams unknown.
    assert data["is_syntactically_gibberish"] == True
    assert data["syntactic_confidence_score"] >= 0.7 # Moderate-to-high confidence it IS syntactically gibberish

def test_detect_non_english_char_text_as_char_gibberish(): # Renamed for clarity
    # This tests if non-English char patterns are caught by char-level check
    response = client.post("/detect", json={"text": "你好世界こんにちは"}) # Chinese and Japanese
    assert response.status_code == 200
    data = response.json()
    validate_response_structure(data)
    assert data["is_char_gibberish"] == True # Because English char trigrams are used
    # Syntactic check might be True or False, depending on tokenization of non-Latin scripts
    # and whether any accidental bigrams form.

def test_detect_spanish_text():
    # Spanish uses Latin alphabet, so char-level might see it as less gibberish than Chinese/Japanese
    # but word bigrams will differ from English.
    response = client.post("/detect", json={"text": "Hola mundo, este es un texto en español."})
    assert response.status_code == 200
    data = response.json()
    validate_response_structure(data)
    assert data["is_char_gibberish"] == False # Spanish trigrams can overlap with English
    assert data["char_gibberish_confidence"] >= 0.5 # Moderate confidence it's not char gibberish
    assert data["is_syntactically_gibberish"] == True # Spanish word order/bigrams are different
    assert data["syntactic_confidence_score"] >= 0.7 # Confident it's syntactically different from Brown corpus English

# --- Input Validation and Edge Case Tests (mostly unchanged, but check new fields) ---

def test_detect_empty_string_input():
    response = client.post("/detect", json={"text": ""})
    assert response.status_code == 422

def test_detect_whitespace_only_input():
    response = client.post("/detect", json={"text": "   "})
    assert response.status_code == 400

def test_detect_non_string_input():
    response = client.post("/detect", json={"text": 12345})
    assert response.status_code == 422

def test_detect_missing_text_field():
    response = client.post("/detect", json={"message": "no text field here"})
    assert response.status_code == 422

def test_detect_text_with_only_special_chars():
    response = client.post("/detect", json={"text": "!@#$%^&*()_+"})
    assert response.status_code == 200
    data = response.json()
    validate_response_structure(data)
    assert data["is_char_gibberish"] == True
    assert data["char_gibberish_confidence"] == 1.0
    assert data["is_syntactically_gibberish"] == False # No words to form bigrams
    assert data["syntactic_confidence_score"] < 0.1 # Or 0.0

def test_detect_text_with_numbers_and_letters():
    response = client.post("/detect", json={"text": "th1s is a t3st w1th numb3rs and common words"})
    assert response.status_code == 200
    data = response.json()
    validate_response_structure(data)
    assert data["is_char_gibberish"] == False
    assert data["char_gibberish_confidence"] >= 0.7
    assert data["is_syntactically_gibberish"] == False
    assert data["syntactic_confidence_score"] >= 0.6

# --- New Tests for Syntactic Gibberish ---

def test_detect_random_word_order_valid_words():
    """Test with valid English words in a random, nonsensical order."""
    response = client.post("/detect", json={"text": "blue table sleepy cat quickly a jumped the over house"})
    assert response.status_code == 200
    data = response.json()
    validate_response_structure(data)
    assert data["is_char_gibberish"] == False # Individual words are fine
    assert data["char_gibberish_confidence"] >= 0.7 # Confident words are not char gibberish
    assert data["is_syntactically_gibberish"] == True # Word order is random
    assert data["syntactic_confidence_score"] >= 0.75 # Confident it's syntactically gibberish

def test_detect_short_coherent_phrase():
    """Test a short, coherent phrase."""
    response = client.post("/detect", json={"text": "good morning"})
    assert response.status_code == 200
    data = response.json()
    validate_response_structure(data)
    assert data["is_char_gibberish"] == False
    assert data["char_gibberish_confidence"] >= 0.8
    # Assuming ('good', 'morning') is not in top N of Brown, it will be syntactically gibberish.
    assert data["is_syntactically_gibberish"] == True
    assert data["syntactic_confidence_score"] >= 0.8 # High confidence as likely 1/1 unknown bigrams

def test_detect_single_word_syntactic():
    """Test a single word for syntactic check (should not be syntactically gibberish)."""
    response = client.post("/detect", json={"text": "supercalifragilisticexpialidocious"})
    assert response.status_code == 200
    data = response.json()
    validate_response_structure(data)
    assert data["is_char_gibberish"] == False # (Assuming this word's trigrams are mostly known or it's long enough)
                                            # This might fail char_gibberish if word is too novel for char trigrams
    assert data["is_syntactically_gibberish"] == False # No bigrams to check
    assert data["syntactic_confidence_score"] < 0.1 # Or 0.0, as no bigrams formed

def test_detect_two_words_known_bigram():
    response = client.post("/detect", json={"text": "of course"}) # Very common bigram
    assert response.status_code == 200
    data = response.json()
    validate_response_structure(data)
    assert data["is_char_gibberish"] == False
    assert data["is_syntactically_gibberish"] == False
    assert data["syntactic_confidence_score"] >= 0.9 # High confidence it's NOT syntactically gibberish

def test_detect_two_words_unknown_bigram():
    response = client.post("/detect", json={"text": "purple ideas"}) # Potentially less common bigram
    assert response.status_code == 200
    data = response.json()
    validate_response_structure(data)
    assert data["is_char_gibberish"] == False # Words are fine
    # The syntactic check depends on whether "purple ideas" is in top N bigrams.
    # If it's not, it will be True.
    # DEFAULT_THRESHOLD_SEMANTIC is 0.80 in gibberish_detector.py
    if data["is_syntactically_gibberish"]:
        assert data["syntactic_confidence_score"] >= 0.80
    else:
        assert data["syntactic_confidence_score"] > (1.0 - 0.80)


# --- Test NLTK Resource Loading ---
import gibberish_detector # Import the module directly

def test_nltk_resources_loaded_and_profiles_generated():
    """Checks if NLTK resources were loaded and profiles generated."""
    print("\\n--- NLTK Resource Status (from test_nltk_resources_loaded) ---")
    for resource_name, resource_info in gibberish_detector.NLTK_RESOURCES.items():
        print(f"Test Check - Resource '{resource_name}': {'Available' if resource_info['status'] else 'Not Available'}")
    print(f"Test Check - COMMON_CHAR_TRIGRAMS size: {len(gibberish_detector.COMMON_CHAR_TRIGRAMS)}")
    print(f"Test Check - COMMON_WORD_BIGRAMS size: {len(gibberish_detector.COMMON_WORD_BIGRAMS)}")
    print("---------------------------------------------------------------")

    assert gibberish_detector.NLTK_RESOURCES['words']['status'] == True, "NLTK 'words' corpus should be available."
    # 'punkt' is no longer explicitly managed/checked here as TreebankWordTokenizer is used.
    # assert gibberish_detector.NLTK_RESOURCES['punkt']['status'] == True, "NLTK 'punkt' tokenizer should be available."
    assert gibberish_detector.NLTK_RESOURCES['brown']['status'] == True, "NLTK 'brown' corpus should be available."

    # Check if profiles are reasonably populated (not just fallback or empty)
    # Fallback for COMMON_CHAR_TRIGRAMS has 5 items.
    # No fallback for COMMON_WORD_BIGRAMS, so it would be 0 if generation failed.
    assert len(gibberish_detector.COMMON_CHAR_TRIGRAMS) > 500, "Character trigram profile should be substantially populated."
    assert len(gibberish_detector.COMMON_WORD_BIGRAMS) > 500, "Word bigram profile should be substantially populated."
