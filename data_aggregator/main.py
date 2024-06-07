from fastapi import FastAPI
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from data_aggregator.bigquery import run_all_queries

app = FastAPI()

def schedule_queries():
    scheduler = AsyncIOScheduler()
    scheduler.add_job(run_all_queries, 'interval', minutes=5)
    scheduler.start()

@app.on_event("startup")
async def startup_event():
    schedule_queries()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
