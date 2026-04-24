import json
import time
import os

# Robust path handling
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
RAW_DATA_DIR = os.path.join(os.path.dirname(SCRIPT_DIR), "raw_data")


# Import role-specific modules
from schema import UnifiedDocument
from process_pdf import extract_pdf_data
from process_transcript import clean_transcript
from process_html import parse_html_catalog
from process_csv import process_sales_csv
from process_legacy_code import extract_logic_from_code
from quality_check import run_quality_gate

# ==========================================
# ROLE 4: DEVOPS & INTEGRATION SPECIALIST
# ==========================================
# Task: Orchestrate the ingestion pipeline and handle errors/SLA.

def _ensure_unified(doc_dict):
    # Validate against schema but keep dict output for JSON serialization
    model = UnifiedDocument(**doc_dict)
    return model.model_dump(mode="json")


def main():
    start_time = time.time()
    final_kb = []
    
    # --- FILE PATH SETUP (Handled for students) ---
    pdf_path = os.path.join(RAW_DATA_DIR, "lecture_notes.pdf")
    trans_path = os.path.join(RAW_DATA_DIR, "demo_transcript.txt")
    html_path = os.path.join(RAW_DATA_DIR, "product_catalog.html")
    csv_path = os.path.join(RAW_DATA_DIR, "sales_records.csv")
    code_path = os.path.join(RAW_DATA_DIR, "legacy_pipeline.py")
    
    output_path = os.path.join(os.path.dirname(SCRIPT_DIR), "processed_knowledge_base.json")
    # ----------------------------------------------

    # TODO: Call each processing function (extract_pdf_data, clean_transcript, etc.)
    # TODO: Run quality gates (run_quality_gate) before adding to final_kb
    # TODO: Save final_kb to output_path using json.dump
    
    # Example:
    # doc = extract_pdf_data(pdf_path)
    # if doc and run_quality_gate(doc):
    #     final_kb.append(doc)

    processors = [
        ("PDF", lambda: [extract_pdf_data(pdf_path)]),
        ("Video", lambda: [clean_transcript(trans_path)]),
        ("HTML", lambda: parse_html_catalog(html_path)),
        ("CSV", lambda: process_sales_csv(csv_path)),
        ("Code", lambda: [extract_logic_from_code(code_path)]),
    ]

    for name, fn in processors:
        try:
            docs = fn() or []
        except Exception as e:
            print(f"[ERROR] Failed processing {name}: {e}")
            continue

        for doc in docs:
            if not doc:
                continue
            try:
                validated = _ensure_unified(doc)
            except Exception as e:
                print(f"[ERROR] Schema validation failed for {name} doc: {e}")
                continue

            if run_quality_gate(validated):
                final_kb.append(validated)

    end_time = time.time()
    try:
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(final_kb, f, ensure_ascii=False, indent=2)
        print(f"Wrote Knowledge Base to: {output_path}")
    except Exception as e:
        print(f"[ERROR] Failed to write output JSON: {e}")

    print(f"Pipeline finished in {end_time - start_time:.2f} seconds.")
    print(f"Total valid documents stored: {len(final_kb)}")


if __name__ == "__main__":
    main()
