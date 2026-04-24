import ast
import re

# ==========================================
# ROLE 2: ETL/ELT BUILDER
# ==========================================
# Task: Extract docstrings and comments from legacy Python code.

def extract_logic_from_code(file_path):
    # --- FILE READING (Handled for students) ---
    with open(file_path, 'r', encoding='utf-8') as f:
        source_code = f.read()
    # ------------------------------------------
    
    # TODO: Use the 'ast' module to find docstrings for functions
    # TODO: (Optional/Advanced) Use regex to find business rules in comments like "# Business Logic Rule 001"
    # TODO: Return a dictionary for the UnifiedDocument schema.

    tree = ast.parse(source_code)

    module_doc = ast.get_docstring(tree) or ""
    fn_docs = []
    for node in tree.body:
        if isinstance(node, ast.FunctionDef):
            doc = ast.get_docstring(node) or ""
            if doc.strip():
                fn_docs.append((node.name, doc.strip()))

    # Extract "Business Logic Rule XXX" blocks from docstrings/comments
    rules = []
    for _, doc in fn_docs:
        for m in re.finditer(r"(Business Logic Rule\s+\d+:[\s\S]*?)(?:\n\s*\n|$)", doc, flags=re.IGNORECASE):
            rules.append(m.group(1).strip())

    # Catch misleading VAT comment vs code (for QA to flag)
    vat_comment_claim = None
    if re.search(r"VAT.*8%", source_code, flags=re.IGNORECASE) or re.search(r"does it does 8%", source_code, flags=re.IGNORECASE):
        vat_comment_claim = 0.08
    vat_code_rate = None
    m = re.search(r"tax_rate\s*=\s*([0-9.]+)", source_code)
    if m:
        try:
            vat_code_rate = float(m.group(1))
        except ValueError:
            vat_code_rate = None

    content_parts = [
        "Extracted legacy business rules and intent from historical Python module.",
        "",
        "=== MODULE DOCSTRING ===",
        module_doc.strip(),
        "",
        "=== FUNCTION DOCSTRINGS ===",
    ]
    for name, doc in fn_docs:
        content_parts.append(f"\n--- {name} ---\n{doc}")
    if rules:
        content_parts.append("\n=== EXTRACTED RULE BLOCKS ===\n" + "\n\n".join(rules))

    return {
        "document_id": "code-legacy-001",
        "content": "\n".join([p for p in content_parts if p is not None]),
        "source_type": "Code",
        "author": "Senior Dev (retired)",
        "timestamp": None,
        "source_metadata": {
            "original_file": "legacy_pipeline.py",
            "extracted_functions": [n for n, _ in fn_docs],
            "business_rules": rules,
            "vat_comment_claim_rate": vat_comment_claim,
            "vat_code_rate": vat_code_rate,
        },
    }

