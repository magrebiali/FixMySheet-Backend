from fastapi import FastAPI, UploadFile, File, Form, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from typing import Optional, Literal
import pandas as pd
import uuid
import os

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # later restrict to your GitHub Pages domain
    allow_methods=["*"],
    allow_headers=["*"],
)

TMP_DIR = "tmp"
os.makedirs(TMP_DIR, exist_ok=True)


@app.get("/")
def health_check():
    return {"status": "FixMySheet API running"}


def read_table(upload: UploadFile) -> pd.DataFrame:
    """
    Read either Excel (.xlsx/.xls) or CSV.
    """
    filename = (upload.filename or "").lower()

    if filename.endswith(".csv"):
        return pd.read_csv(upload.file)

    # Default to Excel
    return pd.read_excel(upload.file)


def normalize_key(series: pd.Series) -> pd.Series:
    # Normalize to string, strip whitespace, and treat NaN as empty
    return series.fillna("").astype(str).str.strip()


def reconcile_files(df_a: pd.DataFrame, df_b: pd.DataFrame, key: str):
    df_a[key] = normalize_key(df_a[key])
    df_b[key] = normalize_key(df_b[key])

    matches = df_a.merge(df_b, on=key, how="inner", suffixes=("_A", "_B"))
    only_a = df_a[~df_a[key].isin(df_b[key])]
    only_b = df_b[~df_b[key].isin(df_a[key])]

    summary = pd.DataFrame(
        {
            "Metric": ["Rows in A", "Rows in B", "Matches", "Only in A", "Only in B"],
            "Count": [len(df_a), len(df_b), len(matches), len(only_a), len(only_b)],
        }
    )

    return matches, only_a, only_b, summary

def dedupe_by_column(df: pd.DataFrame, key_column: str):
    s = df[key_column].fillna("").astype(str).str.strip()

    # Recommended behavior: blanks are NOT treated as duplicatable
    is_blank = s.eq("")

    # Mark ALL occurrences in a duplicate group as duplicate (keep=False)
    dup_mask = s.duplicated(keep=False) & ~is_blank

    out = df.copy()
    out["DuplicateMode"] = "column"
    out["DuplicateKey"] = s
    out["DuplicateFlag"] = dup_mask.map(lambda x: "Duplicate" if x else "Unique")

    duplicates_only = out[out["DuplicateFlag"] == "Duplicate"]
    unique_only = out[out["DuplicateFlag"] == "Unique"]

    summary = pd.DataFrame(
        {
            "Metric": ["Total rows", "Unique rows", "Duplicate rows", "Blank keys"],
            "Count": [len(out), len(unique_only), len(duplicates_only), int(is_blank.sum())],
        }
    )

    return out, duplicates_only, unique_only, summary

def dedupe_by_row(df: pd.DataFrame, ignore_columns: Optional[list[str]] = None, mark_all_in_group: bool = True):
    work = df.copy()

    ignore_columns = ignore_columns or []
    subset_cols = [c for c in work.columns if c not in ignore_columns]

    if not subset_cols:
        raise ValueError("No columns left to compare after ignoring columns.")

    # Normalize strings for stable equality; normalize NaNs too
    for c in subset_cols:
        if work[c].dtype == "object":
            work[c] = work[c].fillna("").astype(str).str.strip()
        else:
            # keep numeric as-is; just normalize NaN so equality is consistent
            work[c] = work[c].where(work[c].notna(), None)

    keep_opt = False if mark_all_in_group else "first"
    dup_mask = work.duplicated(subset=subset_cols, keep=keep_opt)

    out = df.copy()
    out["DuplicateMode"] = "row"
    out["DuplicateFlag"] = dup_mask.map(lambda x: "Duplicate" if x else "Unique")

    duplicates_only = out[out["DuplicateFlag"] == "Duplicate"]
    unique_only = out[out["DuplicateFlag"] == "Unique"]

    summary = pd.DataFrame(
        {
            "Metric": ["Total rows", "Unique rows", "Duplicate rows", "Compared columns"],
            "Count": [len(out), len(unique_only), len(duplicates_only), len(subset_cols)],
        }
    )

    compared_cols_df = pd.DataFrame({"ComparedColumns": subset_cols})

    return out, duplicates_only, unique_only, summary, compared_cols_df


def safe_delete(path: str):
    try:
        if os.path.exists(path):
            os.remove(path)
    except Exception:
        pass


@app.post("/process")
async def process_files(
    background_tasks: BackgroundTasks,
    file_a: UploadFile = File(...),
    file_b: UploadFile = File(...),
    match_column: str = Form(...),
):
    try:
        df_a = read_table(file_a)
        df_b = read_table(file_b)
    except Exception:
        return JSONResponse(status_code=400, content={"error": "Invalid file. Upload .xlsx or .csv"})

    if match_column not in df_a.columns or match_column not in df_b.columns:
        return JSONResponse(
            status_code=400,
            content={
                "error": f"Column '{match_column}' must exist in both files.",
                "columns_in_a": list(map(str, df_a.columns)),
                "columns_in_b": list(map(str, df_b.columns)),
            },
        )

    matches, only_a, only_b, summary = reconcile_files(df_a, df_b, match_column)

    file_id = str(uuid.uuid4())
    output_path = os.path.join(TMP_DIR, f"result_{file_id}.xlsx")

    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        matches.to_excel(writer, sheet_name="Matches", index=False)
        only_a.to_excel(writer, sheet_name="Only_in_File_A", index=False)
        only_b.to_excel(writer, sheet_name="Only_in_File_B", index=False)
        summary.to_excel(writer, sheet_name="Summary", index=False)

    # Delete after response is sent (prevents disk from filling up)
    background_tasks.add_task(safe_delete, output_path)

    return FileResponse(
        output_path,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        filename="FixMySheet_Result.xlsx",
    )

@app.post("/dedupe")
async def dedupe(
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...),
    mode: Literal["column", "row"] = Form(...),
    key_column: Optional[str] = Form(None),
    ignore_columns: Optional[str] = Form(None),
    mark_all_in_group: bool = Form(True),
):
    try:
        df = read_table(file)
    except Exception:
        return JSONResponse(status_code=400, content={"error": "Invalid file. Upload .xlsx or .csv"})

    if df is None or df.empty:
        return JSONResponse(status_code=400, content={"error": "File contains no rows to process."})

    df.columns = [str(c) for c in df.columns]

    if mode == "column":
        if not key_column or not str(key_column).strip():
            return JSONResponse(status_code=400, content={"error": "key_column is required when mode='column'."})

        key_column = str(key_column).strip()

        if key_column not in df.columns:
            return JSONResponse(
                status_code=400,
                content={"error": f"Column '{key_column}' not found.", "columns": df.columns.tolist()},
            )

        out, duplicates_only, unique_only, summary = dedupe_by_column(df, key_column)
        compared_cols_df = pd.DataFrame({"ComparedColumns": [key_column]})

    elif mode == "row":
        ignore_list = []
        if ignore_columns and ignore_columns.strip():
            ignore_list = [c.strip() for c in ignore_columns.split(",") if c.strip()]

        bad_ignores = [c for c in ignore_list if c not in df.columns]
        if bad_ignores:
            return JSONResponse(
                status_code=400,
                content={"error": f"Ignore columns not found: {bad_ignores}", "columns": df.columns.tolist()},
            )

        try:
            out, duplicates_only, unique_only, summary, compared_cols_df = dedupe_by_row(
                df, ignore_columns=ignore_list, mark_all_in_group=mark_all_in_group
            )
        except ValueError as e:
            return JSONResponse(status_code=400, content={"error": str(e)})

    else:
        return JSONResponse(status_code=400, content={"error": "mode must be either 'column' or 'row'."})

    file_id = str(uuid.uuid4())
    output_path = os.path.join(TMP_DIR, f"dedupe_{file_id}.xlsx")

    parameters = pd.DataFrame(
        {
            "Parameter": ["mode", "source_filename", "key_column", "ignore_columns", "mark_all_in_group"],
            "Value": [mode, file.filename or "", key_column or "", ignore_columns or "", str(mark_all_in_group)],
        }
    )

    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        out.to_excel(writer, sheet_name="All_Rows", index=False)
        duplicates_only.to_excel(writer, sheet_name="Duplicates_Only", index=False)
        unique_only.to_excel(writer, sheet_name="Unique_Only", index=False)
        summary.to_excel(writer, sheet_name="Summary", index=False)
        parameters.to_excel(writer, sheet_name="Parameters", index=False)
        compared_cols_df.to_excel(writer, sheet_name="Compared_Columns", index=False)

    background_tasks.add_task(safe_delete, output_path)

    return FileResponse(
        output_path,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        filename="FixMySheet_Dedupe.xlsx",
    )
