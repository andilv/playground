# Plan for Multi-Language Gibberish Detection Support

## 1. Overview

This plan outlines the steps and considerations for extending the current gibberish detection system to support multiple languages. The core idea is to adapt the existing n-gram based statistical approach (character trigrams and word bigrams) for each new language.

## 2. Core Architectural Changes

### 2.1. Language Detection
-   Integrate a language detection library (e.g., `langdetect`, `fastText`).
-   The system must identify the language of the input text as the first step.
-   Decision: Choose a library based on accuracy, performance, and ease of integration.

### 2.2. Language-Specific Profile Management
-   **Storage:** Design a directory structure to store n-gram profiles for each language (e.g., `profiles/<lang_code>/char_trigrams.json`, `profiles/<lang_code>/word_bigrams.json`).
-   **Loading:** Modify `gibberish_detector.py` to load the appropriate profiles based on the detected language.
-   **Profile Generation Scripts:** Create a suite of scripts to generate these profiles from raw text corpora.

### 2.3. Configuration
-   Maintain a configuration file (e.g., `languages.json`) listing supported languages, paths to their profiles, and potentially language-specific thresholds.

## 3. Process for Adding a New Language

For each new language to be supported:

### 3.1. Corpus Acquisition and Preparation
-   **Identify Corpus:** Find a suitable large text corpus for the target language. Prioritize NLTK corpora if available and adequate. Otherwise, seek external sources.
    -   Minimum size: Aim for corpora with at least 1 million words, preferably more, for robust n-gram statistics.
-   **Preprocessing:**
    -   Clean the corpus: Remove boilerplate, markup, etc.
    -   Normalize text: Handle variations in encoding, characters, etc.
    -   Tokenization: Implement or use a language-appropriate word tokenizer. NLTK's `TreebankWordTokenizer` might not be suitable for all languages. Consider sentence tokenization as well.

### 3.2. N-gram Profile Generation
-   **Character Trigrams:**
    -   Extract a comprehensive word list from the corpus.
    -   Generate character trigrams from this word list.
    -   Count frequencies and select the `TOP_N_CHAR_TRIGRAMS` (this N might need tuning per language).
    -   Store the resulting set (e.g., as a JSON list).
-   **Word Bigrams:**
    -   Process the tokenized corpus to extract word bigrams.
    -   Filter out bigrams containing very rare words or stop words (optional but can improve quality).
    -   Count frequencies and select the `TOP_N_WORD_BIGRAMS` (this N might need tuning per language).
    -   Store the resulting set (e.g., as a JSON list of lists/tuples).

### 3.3. Threshold Tuning
-   Determine optimal `DEFAULT_THRESHOLD_CHAR` and `DEFAULT_THRESHOLD_SYNTACTIC` (renamed from `DEFAULT_THRESHOLD_SEMANTIC` for clarity in a multi-language context) for the new language through empirical testing.
-   This will require a test set of gibberish and non-gibberish sentences in that language.

### 3.4. Integration and Testing
-   Add the language code and profile paths to the configuration.
-   Develop specific test cases in `tests/test_main.py` for the new language, covering:
    -   Valid text.
    -   Character-level gibberish.
    -   Syntactic gibberish.
    -   The "verdant somnolence" equivalent (semantically absurd but grammatically plausible).

## 4. Candidate Languages for Initial Support (Based on NLTK Corpora)

This list prioritizes languages with more substantial corpora in NLTK that could lend themselves to n-gram profiling. The actual suitability would need verification.

-   **Spanish (`es`):**
    -   Corpora: `cess_esp`
    -   Considerations: Standard tokenization usually works.
-   **Portuguese (`pt`):**
    -   Corpora: `floresta`, `mac_morpho`
-   **German (`de`):**
    -   Corpora: `conll2000` (shared task data), `tiger` (may need conversion)
    -   Considerations: Compound words might need special handling or larger N for n-grams.
-   **Dutch (`nl`):**
    -   Corpora: `alpino`, `conll2002`
-   **French (`fr`):**
    -   NLTK has some resources like `udhr` (Universal Declaration of Human Rights) but a larger dedicated corpus might be needed. External corpora are widely available.
-   **Italian (`it`):**
    -   NLTK `udhr`. External corpora recommended for robust profiles.
-   **Other Potential Languages (requiring more investigation for corpus size/quality in NLTK or relying on external corpora):**
    -   Russian (`ru`)
    -   Chinese (`zh`) - Requires specialized tokenization. NLTK `sinica_treebank`.
    -   Japanese (`ja`) - Requires specialized tokenization.
    -   Arabic (`ar`) - Requires right-to-left text handling and specialized tokenization.
    -   Hindi (`hi`) - NLTK `indian` corpus.

*(Disclaimer: The quality and size of NLTK corpora can vary. External, larger corpora are often preferred for building robust production models.)*

## 5. Refinement of Gibberish Definition

-   The concept of "syntactic gibberish" based on n-gram rarity might behave differently across languages depending on morphological richness or word order flexibility.
-   The "semantic gibberish" (like "verdant somnolence") detection, if pursued beyond simple n-gram rarity, would require separate, more advanced modeling (e.g., using multilingual embeddings like Sentence-BERT and appropriate comparison strategies). This plan focuses on extending the current n-gram approach.

## 6. Testing Strategy for Multi-Language Support

-   For each supported language:
    -   A dedicated test suite (`test_main_<lang_code>.py` or sections in `test_main.py`).
    -   Test cases for character gibberish, syntactic gibberish, valid sentences, and known tricky cases.
-   Cross-language tests: Ensure that text in language A is flagged as gibberish if the system is expecting language B (if no language detection or strict mode).

## 7. Maintenance
-   Regularly review and update language profiles as corpora evolve or better ones become available.
-   Monitor the performance of language detection.
-   Update dependencies (language detection libraries, NLTK).

This plan provides a roadmap. Each step, especially corpus preparation and profile generation for a new language, requires careful execution and validation.
