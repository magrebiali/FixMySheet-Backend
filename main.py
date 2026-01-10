from fastapi import FastAPI, UploadFile, File, Form, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
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
