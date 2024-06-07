from __future__ import annotations
import json
import logging
from typing import Any
import uvicorn
from fastapi import FastAPI, Request, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import ValidationError
from classes import EventPayload, MetricsRequest, MetricsResponse
from bigquery import get_bigquery_metrics, get_bigquery_metrics_parallel, write_event_bigquery

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load configuration from environment variables
import os
# This is a list of shopify stores
SHOPIFY_ORIGIN: list[str] = os.getenv("SHOPIFY_ORIGIN", ["https://shopify-domain.myshopify.com"])

# Create FastAPI app instance
app = FastAPI()

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=SHOPIFY_ORIGIN,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
async def root() -> dict:
    """
    Root endpoint.
    """
    return {"message": "Rain check from the server!"}

@app.options("/ingest-gcp")
async def handle_options(request: Request) -> dict:
    """
    Handle OPTIONS request for CORS preflight.
    """
    return {"status": "ok"}

@app.post("/ingest-gcp")
async def ingest_event_gcp(request: Request) -> None:
    """
    Endpoint to ingest events to GCP.
    """
    try:
        body = await request.json()
        event_payload = EventPayload(**body)
        write_event_bigquery(event_payload)
        logger.info("Event ingested to GCP successfully")
    except ValidationError as e:
        logger.error(f"Validation error: {e}")
        raise HTTPException(status_code=400, detail="Invalid event payload")
    except json.JSONDecodeError as e:
        logger.error(f"JSON decode error: {e}")
        raise HTTPException(status_code=400, detail="Invalid JSON payload")
    except Exception as e:
        logger.error(f"Error ingesting event to GCP: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")
    

@app.post("/get_metrics")
async def get_metrics(request: MetricsRequest) -> MetricsResponse:
    metrics = get_bigquery_metrics(request)
    return metrics

@app.post("/get_metrics_parallel")
async def get_metrics_parallel(request: MetricsRequest) -> MetricsResponse:
    metrics = await get_bigquery_metrics_parallel(request)
    return metrics


if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000)
