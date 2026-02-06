from fastapi import FastAPI, UploadFile, File, Form, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from typing import Optional, Literal
import pandas as pd
import uuid
import os

print("🧨 main.py loaded")

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


# =========================
# File reading helpers
# =========================
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


# =========================
# Reconcile logic
# =========================
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

    background_tasks.add_task(safe_delete, output_path)

    return FileResponse(
        output_path,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        filename="FixMySheet_Result.xlsx",
    )


# =========================
# Dedupe (Audit-friendly)
# =========================
KeepPolicy = Literal["mark_all", "keep_first", "keep_last"]
DedupeMode = Literal["column", "row"]


def _normalize_text_series(
    s: pd.Series,
    ignore_case: bool,
    ignore_whitespace: bool,
) -> pd.Series:
    """
    Normalize a text-like series for stable equality comparisons.
    """
    s = s.fillna("").astype(str)

    # strip edges always
    s = s.str.strip()

    if ignore_whitespace:
        # remove ALL whitespace (spaces, tabs, newlines)
        s = s.str.replace(r"\s+", "", regex=True)

    if ignore_case:
        s = s.str.lower()

    return s


def _make_row_keys(
    df: pd.DataFrame,
    subset_cols: list[str],
    ignore_case: bool,
    ignore_whitespace: bool,
) -> pd.Series:
    """
    Build a stable per-row comparison key from selected columns.
    """
    work = df[subset_cols].copy()

    for c in subset_cols:
        if work[c].dtype == "object":
            work[c] = _normalize_text_series(work[c], ignore_case, ignore_whitespace)
        else:
            work[c] = work[c].where(work[c].notna(), "")

        work[c] = work[c].astype(str)

    delim = "\u001f"  # Unit Separator
    return work.agg(lambda r: delim.join(r.values.tolist()), axis=1)


def _audit_duplicate_groups(
    *,
    df: pd.DataFrame,
    group_key: pd.Series,
    display_key: Optional[pd.Series],
    keep_policy: KeepPolicy,
    treat_blank_as_unique: bool = True,
) -> pd.DataFrame:
    """
    Compute audit-friendly duplicate annotations.

    Adds:
      - DuplicateGroupID (same for all rows in group)
      - DuplicateCount (size of the group)
      - DuplicateFirstSeenRow (1-based row number for first occurrence)
      - DuplicateFlag (Unique / Kept / Duplicate)
      - DuplicateKey (DISPLAY key - human-friendly)
    """
    out = df.copy()
    out_index = out.index

    # The key used for grouping (can be modified to prevent blanks grouping)
    internal_key = group_key.fillna("").astype(str)

    # The key shown to the user (never show internal blank hack strings)
    if display_key is None:
        display_key = group_key.fillna("").astype(str)
    else:
        display_key = display_key.fillna("").astype(str)

    is_blank = internal_key.eq("")

    # Optional: blanks should not form duplicate groups
    if treat_blank_as_unique:
        internal_key = internal_key.where(~is_blank, other="__BLANK__ROW__" + out_index.astype(str))

    counts = internal_key.map(internal_key.value_counts())
    in_dup_group = counts.gt(1)

    # First-seen row number (1-based), per group
    row_number = pd.Series(range(1, len(out) + 1), index=out_index)
    first_seen = row_number.groupby(internal_key).transform("min")

    # Group IDs
    codes, _ = pd.factorize(internal_key, sort=False)
    group_id_str = pd.Series(codes, index=out_index).map(lambda x: f"G{(x+1):06d}")
    group_id_str = group_id_str.where(in_dup_group, other="")

    # Flagging based on keep_policy
    if keep_policy == "mark_all":
        flag = pd.Series("Unique", index=out_index)
        flag = flag.where(~in_dup_group, other="Duplicate")
    elif keep_policy == "keep_first":
        is_dup_row = internal_key.duplicated(keep="first")
        flag = pd.Series("Unique", index=out_index)
        flag = flag.where(~in_dup_group, other="Kept")
        flag = flag.where(~is_dup_row, other="Duplicate")
    elif keep_policy == "keep_last":
        is_dup_row = internal_key.duplicated(keep="last")
        flag = pd.Series("Unique", index=out_index)
        flag = flag.where(~in_dup_group, other="Kept")
        flag = flag.where(~is_dup_row, other="Duplicate")
    else:
        raise ValueError("keep_policy must be: mark_all | keep_first | keep_last")

    # Clean display for blank keys
    # (show blank, not internal __BLANK__ROW__)
    display_key = display_key.where(~is_blank, other="")

    out["DuplicateKey"] = display_key
    out["DuplicateGroupID"] = group_id_str
    out["DuplicateCount"] = counts.astype(int)
    out["DuplicateFirstSeenRow"] = first_seen.astype(int)
    out["DuplicateFlag"] = flag

    return out


# =========================
# Deduplication Endpoint
# =========================
@app.post("/dedupe")
async def dedupe(
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...),
    mode: DedupeMode = Form(...),

    # Options
    keep_policy: KeepPolicy = Form("mark_all"),
    ignore_case: bool = Form(False),
    ignore_whitespace: bool = Form(False),

    # Used when mode="column"
    key_column: Optional[str] = Form(None),

    # Used when mode="row"
    ignore_columns: Optional[str] = Form(None),  # comma-separated list
):
    # 1) Read file
    try:
        df = read_table(file)
    except Exception:
        return JSONResponse(status_code=400, content={"error": "Invalid file. Upload .xlsx or .csv"})

    if df is None or df.empty:
        return JSONResponse(status_code=400, content={"error": "File contains no rows to process."})

    df.columns = [str(c) for c in df.columns]

    if keep_policy not in ("mark_all", "keep_first", "keep_last"):
        return JSONResponse(
            status_code=400,
            content={"error": "keep_policy must be: mark_all | keep_first | keep_last"},
        )

    # 2) Build group key depending on mode
    if mode == "column":
        if not key_column or not str(key_column).strip():
            return JSONResponse(status_code=400, content={"error": "key_column is required when mode='column'."})

        key_column = str(key_column).strip()
        if key_column not in df.columns:
            return JSONResponse(
                status_code=400,
                content={"error": f"Column '{key_column}' not found.", "columns": df.columns.tolist()},
            )

        col_raw = df[key_column]
        col_norm = _normalize_text_series(col_raw, ignore_case, ignore_whitespace)

        out = _audit_duplicate_groups(
            df=df,
            group_key=col_norm,          # internal grouping uses normalized
            display_key=col_raw,         # show original values (cleaned below for blanks)
            keep_policy=keep_policy,
            treat_blank_as_unique=True,
        )
        out.insert(0, "DuplicateMode", "column")

    elif mode == "row":
        ignore_list: list[str] = []
        if ignore_columns and ignore_columns.strip():
            ignore_list = [c.strip() for c in ignore_columns.split(",") if c.strip()]

        bad_ignores = [c for c in ignore_list if c not in df.columns]
        if bad_ignores:
            return JSONResponse(
                status_code=400,
                content={"error": f"Ignore columns not found: {bad_ignores}", "columns": df.columns.tolist()},
            )

        subset_cols = [c for c in df.columns if c not in ignore_list]
        if not subset_cols:
            return JSONResponse(status_code=400, content={"error": "No columns left to compare after ignoring columns."})

        row_keys = _make_row_keys(df, subset_cols, ignore_case, ignore_whitespace)

        out = _audit_duplicate_groups(
            df=df,
            group_key=row_keys,
            display_key=None,  # display_key defaults to group_key for row mode
            keep_policy=keep_policy,
            treat_blank_as_unique=False,
        )
        out.insert(0, "DuplicateMode", "row")

    else:
        return JSONResponse(status_code=400, content={"error": "mode must be either 'column' or 'row'."})

    # 3) Output workbook — ONLY All_Rows
    file_id = str(uuid.uuid4())
    output_path = os.path.join(TMP_DIR, f"dedupe_{file_id}.xlsx")

    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        out.to_excel(writer, sheet_name="All_Rows", index=False)

    background_tasks.add_task(safe_delete, output_path)

    return FileResponse(
        output_path,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        filename="FixMySheet_Dedupe.xlsx",
    )
