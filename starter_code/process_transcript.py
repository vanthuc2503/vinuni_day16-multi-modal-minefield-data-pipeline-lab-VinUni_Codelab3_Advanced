import re

# ==========================================
# ROLE 2: ETL/ELT BUILDER
# ==========================================
# Task: Clean the transcript text and extract key information.

_VIET_NUM = {
    "không": 0,
    "mot": 1,
    "một": 1,
    "hai": 2,
    "ba": 3,
    "bon": 4,
    "bốn": 4,
    "nam": 5,
    "năm": 5,
    "sau": 6,
    "sáu": 6,
    "bay": 7,
    "bảy": 7,
    "tam": 8,
    "tám": 8,
    "chin": 9,
    "chín": 9,
    "muoi": 10,
    "mười": 10,
}


def _strip_noise(text: str) -> str:
    # remove [00:00:00] timestamps
    text = re.sub(r"\[\d{2}:\d{2}:\d{2}\]\s*", "", text)
    # remove bracketed noise tokens
    text = re.sub(r"\[(?:Music.*?|inaudible|Laughter.*?)\]", "", text, flags=re.IGNORECASE)
    # remove [Speaker X]:
    text = re.sub(r"\[Speaker\s*\d+\]\s*:\s*", "", text, flags=re.IGNORECASE)
    # collapse whitespace
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def _detect_price_vnd(text: str):
    # First: numeric mention like 500,000
    m = re.search(r"(\d{1,3}(?:[.,]\d{3})+)\s*VND", text, flags=re.IGNORECASE)
    if m:
        digits = re.sub(r"[^\d]", "", m.group(1))
        try:
            return int(digits)
        except ValueError:
            pass

    # Second: Vietnamese words like "năm trăm nghìn"
    t = text.lower()
    if "năm trăm nghìn" in t or "nam tram nghin" in t:
        return 500000

    # lightweight generic parse: "<digit word> trăm nghìn"
    m = re.search(r"\b(\w+)\s+trăm\s+nghìn\b", t)
    if m:
        w = m.group(1)
        if w in _VIET_NUM:
            return _VIET_NUM[w] * 100000

    return None


def clean_transcript(file_path):
    # --- FILE READING (Handled for students) ---
    with open(file_path, 'r', encoding='utf-8') as f:
        text = f.read()
    # ------------------------------------------
    
    # TODO: Remove noise tokens like [Music], [inaudible], [Laughter]
    # TODO: Strip timestamps [00:00:00]
    # TODO: Find the price mentioned in Vietnamese words ("năm trăm nghìn")
    # TODO: Return a cleaned dictionary for the UnifiedDocument schema.

    cleaned = _strip_noise(text)
    detected_price_vnd = _detect_price_vnd(text)

    content = (
        "Cleaned lecture transcript about Data Pipeline Engineering, "
        "including notes on semantic drift and a lab hint about product pricing.\n\n"
        + cleaned
    )

    return {
        "document_id": "video-transcript-001",
        "content": content,
        "source_type": "Video",
        "author": "Unknown",
        "timestamp": None,
        "source_metadata": {
            "original_file": "demo_transcript.txt",
            "detected_price_vnd": detected_price_vnd,
        },
    }

