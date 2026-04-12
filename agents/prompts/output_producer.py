OUTPUT_PRODUCER_TABLES_PROMPT = """
You are generating Python code to produce CSV and Excel tables for an IO economics replication.

## Rules
- Output ONLY raw Python code. No markdown fences. No explanation.
- All file paths MUST be absolute (use the exact paths given below).
- Save every table as BOTH .csv AND .xlsx.
- Use EXACT column names from the data preview — do not rename or abbreviate.
- Do NOT use plt, seaborn, or any visualisation library.
- Imports at the top. No functions — flat sequential script.
- If a table requires aggregation (e.g. country totals), compute it; do not skip.
"""

OUTPUT_PRODUCER_FIGURES_PROMPT = """
You are generating Python code to produce charts and figures for an IO economics replication.

## Rules
- Output ONLY raw Python code. No markdown fences. No explanation.
- All file paths MUST be absolute (use the exact paths given below).
- Save every figure as BOTH .png (dpi=150) AND .pdf.
- Use EXACT column names from the data preview — do not rename.
- Use matplotlib only (no seaborn, no plotly).
- Call plt.close() after saving each figure.
- Imports at the top. No functions — flat sequential script.
- grouped_bar: side-by-side bars, rotated x-labels (rotation=45, ha='right'), legend.
- stacked_bar: stacked bars per country/category.
- heatmap: use imshow or matshow with colorbar.
"""

OUTPUT_PRODUCER_FIX_PROMPT = """
The Python script below failed with an error. Return ONLY the corrected Python code.
No explanation. No markdown fences. Fix ONLY the error — do not rewrite unrelated parts.
"""
