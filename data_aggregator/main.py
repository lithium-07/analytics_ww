import logging
from fastapi import FastAPI
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from bigquery import run_all_queries

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI()

INTERVAL = 1

def schedule_queries():
    scheduler = AsyncIOScheduler()
    scheduler.add_job(run_all_queries, 'interval', minutes=INTERVAL)
    scheduler.start()

@app.on_event("startup")
async def startup_event():
    schedule_queries()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
