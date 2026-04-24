import google.generativeai as genai
import os
import json
import time
import re
from dotenv import load_dotenv

load_dotenv()
genai.configure(api_key=os.getenv("GEMINI_API_KEY"))

def _call_with_backoff(fn, *, max_attempts=6, base_delay_s=1.0):
    delay = base_delay_s
    last_err = None
    for attempt in range(1, max_attempts + 1):
        try:
            return fn()
        except Exception as e:
            last_err = e
            msg = str(e).lower()
            # Gemini can surface rate limiting as "429" or "resource exhausted"
            is_rate_limited = ("429" in msg) or ("resource_exhausted" in msg) or ("rate limit" in msg)
            if not is_rate_limited or attempt == max_attempts:
                raise
            print(f"[WARN] Rate limited by Gemini (attempt {attempt}/{max_attempts}). Backing off {delay:.1f}s...")
            time.sleep(delay)
            delay = min(delay * 2, 20.0)
    raise last_err


def _extract_json_object(text: str) -> dict:
    if not text:
        raise ValueError("Empty Gemini response")

    t = text.strip()
    # Remove markdown fences if present
    t = re.sub(r"^\s*```(?:json)?\s*", "", t, flags=re.IGNORECASE)
    t = re.sub(r"\s*```\s*$", "", t)

    try:
        return json.loads(t)
    except Exception:
        pass

    # Fallback: grab the first {...} block
    m = re.search(r"\{[\s\S]*\}", t)
    if not m:
        raise ValueError("Could not find JSON object in Gemini response")
    return json.loads(m.group(0))


def extract_pdf_data(file_path):
    if not os.path.exists(file_path):
        print(f"Error: File not found at {file_path}")
        return None
        
    # Thay đổi model name để tránh lỗi 404 trên các phiên bản API cũ
    model = genai.GenerativeModel('gemini-2.5-flash')
    
    print(f"Uploading {file_path} to Gemini...")
    try:
        pdf_file = _call_with_backoff(lambda: genai.upload_file(path=file_path))
    except Exception as e:
        print(f"Failed to upload file to Gemini: {e}")
        return None
        
    prompt = """
Analyze this document and extract structured metadata.
Output exactly as a JSON object matching this exact format:
{
    "document_id": "pdf-doc-001",
    "content": "Title: [title]\\nAuthor: [author]\\nMain topics: [comma-separated topics]\\nSummary: [3-sentence summary]\\nTables: [brief table notes]",
    "source_type": "PDF",
    "author": "[Insert author name here]",
    "timestamp": null,
    "source_metadata": {
        "original_file": "lecture_notes.pdf",
        "title": "[title]",
        "main_topics": ["topic1", "topic2"],
        "tables": ["table1", "table2"]
    }
}
"""
    
    print("Generating content from PDF using Gemini...")
    try:
        response = _call_with_backoff(lambda: model.generate_content([pdf_file, prompt]))
    except Exception as e:
        print(f"Failed to generate content from PDF via Gemini: {e}")
        return None

    try:
        extracted_data = _extract_json_object(getattr(response, "text", "") or "")
    except Exception as e:
        print(f"Failed to parse Gemini JSON response: {e}")
        return None

    # Ensure minimum required fields exist
    extracted_data.setdefault("document_id", "pdf-doc-001")
    extracted_data.setdefault("source_type", "PDF")
    extracted_data.setdefault("author", extracted_data.get("author") or "Unknown")
    extracted_data.setdefault("timestamp", None)
    extracted_data.setdefault("source_metadata", {})
    extracted_data["source_metadata"].setdefault("original_file", os.path.basename(file_path))
    return extracted_data
