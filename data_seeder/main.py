from fastapi import FastAPI
from apscheduler.schedulers.background import BackgroundScheduler
from analytics_ww.data_seeder.utils import generate_and_insert_events

SCHEDULER_DURATION = 1

app = FastAPI()

# Scheduler setup
scheduler = BackgroundScheduler()
scheduler.add_job(generate_and_insert_events, 'interval', minutes=SCHEDULER_DURATION)
scheduler.start()

@app.get("/")
def read_root():
    return {"message": "FastAPI server is running and inserting data into BigQuery every 5 minutes"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
