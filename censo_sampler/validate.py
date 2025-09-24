# coding: utf-8
"""
Lightweight validations (non-fatal for now) to mirror original assumptions.
"""

def validate_columns(ddf, required_cols):
    missing = [c for c in required_cols if c not in ddf.columns]
    if missing:
        print(f"[warn] Missing expected columns: {missing}")
    return True
