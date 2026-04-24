# ==========================================
# ROLE 3: OBSERVABILITY & QA ENGINEER
# ==========================================
# Task: Implement quality gates to reject corrupt data or logic discrepancies.

def run_quality_gate(document_dict):
    # TODO: Reject documents with 'content' length < 20 characters
    # TODO: Reject documents containing toxic/error strings (e.g., 'Null pointer exception')
    # TODO: Flag discrepancies (e.g., if tax calculation comment says 8% but code says 10%)
    
    # Return True if pass, False if fail.

    if not isinstance(document_dict, dict):
        return False

    content = str(document_dict.get("content", "") or "")
    if len(content.strip()) < 20:
        return False

    toxic_markers = [
        "Null pointer exception",
        "Traceback (most recent call last)",
        "Segmentation fault",
    ]
    for marker in toxic_markers:
        if marker.lower() in content.lower():
            return False

    # Discrepancy flagging: don't fail the doc, but warn loudly.
    meta = document_dict.get("source_metadata") or {}
    try:
        claim = meta.get("vat_comment_claim_rate")
        code_rate = meta.get("vat_code_rate")
        if claim is not None and code_rate is not None and float(claim) != float(code_rate):
            print(
                f"[WARN] Discrepancy detected in legacy tax calc: comment says {claim}, code uses {code_rate} "
                f"(doc_id={document_dict.get('document_id')})"
            )
    except Exception:
        # Gate should never crash the pipeline
        pass

    return True
