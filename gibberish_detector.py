import re
import nltk
from collections import Counter

# --- NLTK Resource Management ---
NLTK_RESOURCES = {
    'words': {'path': 'corpora/words', 'status': False},
    # 'punkt' tokenizer is no longer explicitly managed here as TreebankWordTokenizer is used.
    'brown': {'path': 'corpora/brown', 'status': False}
}

def download_nltk_resource(resource_name, resource_info):
    try:
        nltk.data.find(resource_info['path'])
        # print(f"NLTK resource '{resource_name}' already available.") # Less verbose
        resource_info['status'] = True
    except LookupError:
        print(f"NLTK resource '{resource_name}' not found. Attempting to download...")
        try:
            nltk.download(resource_name, quiet=True)
            nltk.data.find(resource_info['path'])
            print(f"NLTK resource '{resource_name}' downloaded and verified.")
            resource_info['status'] = True
        except Exception as e:
            print(f"Error downloading or verifying NLTK resource '{resource_name}': {e}")
            resource_info['status'] = False
    return resource_info['status']

def ensure_all_nltk_resources():
    print("Checking NLTK resources...")
    for resource_name, resource_info in NLTK_RESOURCES.items():
        download_nltk_resource(resource_name, resource_info)
    print("--- NLTK Resource Status ---") # Added header for clarity
    for resource_name, resource_info in NLTK_RESOURCES.items():
        print(f"Resource '{resource_name}': {'Available' if resource_info['status'] else 'Not Available'}") # Restored verbosity
    print("----------------------------")

ensure_all_nltk_resources()

# --- Character-level Gibberish Detection ---
DEFAULT_THRESHOLD_CHAR = 0.7
TOP_N_CHAR_TRIGRAMS = 3000
COMMON_CHAR_TRIGRAMS = set() # Defined below

def get_char_trigrams_from_word_list(word_list: list[str], min_word_len: int = 3) -> list[str]:
    all_trigrams = []
    for word in word_list:
        cleaned_word = re.sub(r'[^a-z]', '', word.lower())
        if len(cleaned_word) >= min_word_len:
            for i in range(len(cleaned_word) - min_word_len + 1):
                all_trigrams.append(cleaned_word[i:i+3])
    return all_trigrams

def generate_char_trigram_profile(top_n: int) -> set[str]:
    print("Attempting to generate character trigram profile...") # Changed print
    if not NLTK_RESOURCES['words']['status']:
        print("NLTK 'words' corpus not available for char trigram profile. Returning empty set.")
        return set()
    try:
        english_words = nltk.corpus.words.words()
        print(f"Successfully loaded 'words' corpus for char trigram profile (approx. {len(english_words)} words).")
    except Exception as e:
        print(f"Error accessing NLTK 'words' corpus: {e}. Returning empty set for char trigrams.")
        return set()
    valid_words = [word.lower() for word in english_words if word.isalpha() and len(word) >= 3]
    trigrams_list = get_char_trigrams_from_word_list(valid_words)
    if not trigrams_list:
        print("No valid trigrams generated from 'words' corpus for char profile.")
        return set()
    trigram_counts = Counter(trigrams_list)
    profile = {trigram for trigram, count in trigram_counts.most_common(top_n)}
    print(f"Generated character trigram profile with {len(profile)} unique trigrams.")
    return profile

if NLTK_RESOURCES['words']['status']:
    COMMON_CHAR_TRIGRAMS = generate_char_trigram_profile(top_n=TOP_N_CHAR_TRIGRAMS)
if not COMMON_CHAR_TRIGRAMS:
    print("Warning: Character trigram profile empty. Using fallback for char gibberish.")
    COMMON_CHAR_TRIGRAMS = {'the', 'ing', 'and', 'her', 'ent'}

def get_char_trigrams_from_text(text: str) -> set: # Renamed
    # (Implementation as before)
    if not text: return set()
    text_cleaned = re.sub(r'[^a-z\s]', '', text.lower())
    words = text_cleaned.split()
    trigrams = set()
    for word in words:
        if len(word) >= 3:
            for i in range(len(word) - 2):
                trigrams.add(word[i:i+3])
    return trigrams

def check_char_gibberish(text_to_check: str) -> tuple[bool, float]: # Renamed
    # (Implementation as before)
    if not COMMON_CHAR_TRIGRAMS: return False, 0.0
    if not text_to_check or not isinstance(text_to_check, str) or not text_to_check.strip(): return True, 1.0
    input_trigrams = get_char_trigrams_from_text(text_to_check) # Use renamed function
    if not input_trigrams: return True, 1.0
    unknown_trigrams = {trigram for trigram in input_trigrams if trigram not in COMMON_CHAR_TRIGRAMS}
    unknown_trigram_percentage = len(unknown_trigrams) / len(input_trigrams)
    is_gibberish_result = unknown_trigram_percentage >= DEFAULT_THRESHOLD_CHAR
    confidence = unknown_trigram_percentage if is_gibberish_result else 1.0 - unknown_trigram_percentage
    return is_gibberish_result, max(0.0, min(1.0, confidence))

# --- Semantic Gibberish Detection (Word N-grams) ---
DEFAULT_THRESHOLD_SEMANTIC = 0.80 # Adjusted based on typical observation
TOP_N_WORD_BIGRAMS = 60000 # Increased from 40000
COMMON_WORD_BIGRAMS = set() # Defined below

def generate_word_bigram_profile(corpus_name: str = 'brown', top_n: int = TOP_N_WORD_BIGRAMS) -> set[tuple[str, str]]:
    print(f"Attempting to generate word bigram profile from NLTK '{corpus_name}' corpus...") # Changed print
    if not NLTK_RESOURCES.get(corpus_name, {}).get('status'): # Removed 'punkt' check
        print(f"NLTK '{corpus_name}' corpus not available for word bigram profile. Returning empty set.")
        return set()
    try:
        corpus_sents = getattr(nltk.corpus, corpus_name).sents()
        print(f"Successfully loaded '{corpus_name}' corpus for word bigram profile.")
    except Exception as e:
        print(f"Error accessing NLTK '{corpus_name}' sents: {e}. Returning empty set for word bigrams.")
        return set()
    all_bigrams = []
    for sent in corpus_sents:
        words = [word.lower() for word in sent if word.isalpha() and len(word) > 1]
        if len(words) >= 2: all_bigrams.extend(list(nltk.bigrams(words)))
    if not all_bigrams:
        print(f"No valid bigrams generated from '{corpus_name}' corpus for word profile.")
        return set()
    bigram_counts = Counter(all_bigrams)
    profile = {bigram for bigram, count in bigram_counts.most_common(top_n)}
    print(f"Generated word bigram profile with {len(profile)} unique bigrams from '{corpus_name}'.")
    return profile

if NLTK_RESOURCES['brown']['status']: # Removed 'punkt' check
    COMMON_WORD_BIGRAMS = generate_word_bigram_profile()
if not COMMON_WORD_BIGRAMS:
    print("Warning: Word bigram profile is empty. Semantic analysis will be disabled/unreliable.")

def check_random_word_order(text_to_check: str) -> tuple[bool, float]:
    """
    Checks if the given text exhibits random word order based on word bigram analysis.
    Returns: (is_syntactically_gibberish, confidence_score)
    """
    if not COMMON_WORD_BIGRAMS: # Removed 'punkt' status check
        print("DEBUG: Word bigram profile not available. Semantic check skipped.")
        return False, 0.0 # Cannot perform check reliably

    if not text_to_check or not isinstance(text_to_check, str):
        return False, 0.0 # Not applicable or invalid input for this check

    text_stripped = text_to_check.strip()
    if not text_stripped:
        return False, 0.0 # Empty text has no word order to check

    try:
        from nltk.tokenize import TreebankWordTokenizer # Import here or at top of file
        tokenizer = TreebankWordTokenizer()
        words = tokenizer.tokenize(text_stripped)
        cleaned_words = [word.lower() for word in words if word.isalpha() and len(word) > 1]
        # print(f"DEBUG check_random_word_order: Number of cleaned words: {len(cleaned_words)}") # Optional: keep if still debugging word counts
    except Exception as e:
        print(f"DEBUG: Error during tokenization/cleaning: {e}. Skipping semantic check.")
        return False, 0.0

    if len(cleaned_words) < 2:
        # Not enough words to form bigrams, so it's not "random order" by this method's definition.
        # Could be a single word or very short phrase which is syntactically simple.
        print(f"DEBUG: Not enough cleaned words ({len(cleaned_words)}) to form bigrams. Skipping semantic check.")
        return False, 0.0

    input_bigrams = list(nltk.bigrams(cleaned_words))
    if not input_bigrams: # Should be covered by len(cleaned_words) < 2, but as a safeguard
        print(f"DEBUG: No input bigrams formed from cleaned_words: {cleaned_words}. Skipping semantic check.")
        return False, 0.0

    unknown_bigrams_count = 0
    for bigram in input_bigrams:
        if bigram not in COMMON_WORD_BIGRAMS:
            unknown_bigrams_count += 1

    unknown_bigram_percentage = unknown_bigrams_count / len(input_bigrams)

    is_random_order = unknown_bigram_percentage >= DEFAULT_THRESHOLD_SEMANTIC

    confidence = unknown_bigram_percentage if is_random_order else 1.0 - unknown_bigram_percentage

    return is_random_order, max(0.0, min(1.0, confidence))
