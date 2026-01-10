from fastapi import FastAPI, UploadFile, File, Form
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
import pandas as pd
import uuid
import os

app = FastAPI()

# Allow frontend to call API
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # later restrict
    allow_methods=["*"],
    allow_headers=["*"],
)

TMP_DIR = "tmp"
os.makedirs(TMP_DIR, exist_ok=True)


@app.get("/")
def health_check():
    return {"status": "FixMySheet API running"}