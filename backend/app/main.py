# backend/app/main.py
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from .routes import query as query_router
import os
from dotenv import load_dotenv

load_dotenv()

app = FastAPI(title="Ecom LLM Analytics Backend")

# CORS - allow local frontend during dev
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # in prod restrict this
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(query_router.router, prefix="/api")

@app.get("/")
async def root():
    return {"status": "ok"}
