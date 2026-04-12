OUTPUT_PRODUCER_SYSTEM_PROMPT = """
Produce all tables and figures from the spec.

Rules:
- Write ONE script producing ALL outputs. Call execute_python once.
- Use EXACT column names from the data preview. Do not rename.
- If you need the full contents of a file, call read_file once — do not explore.
- Tables → {run_dir}/outputs/tables/<id>.csv + .xlsx
- Figures → {run_dir}/outputs/figures/<id>.png + .pdf
- Sort where spec says sort. Iterate spec["outputs"] dynamically.
- grouped_bar: side-by-side bars per country, rotated labels, legend.
- stacked_bar: stacked bars, overlay series on twinx if present.
- matrix tables: save as-is (index preserved).
- Use matplotlib + openpyxl. No other deps.

If the script fails, fix it in ONE follow-up call. No exploration.
"""
