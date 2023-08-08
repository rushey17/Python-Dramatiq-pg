import dramatiq
import psycopg2.pool
from fastapi import FastAPI
from dramatiq_pg import PostgresBroker
import requests
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from apscheduler.triggers.cron import CronTrigger
import time
import os

os.environ["DRAMATIQ_DEBUG"] = "1"


dramatiq.set_broker(PostgresBroker(url="postgresql://postgres:postgres@localhost:5432/postgres"))
app = FastAPI()
jobstore = SQLAlchemyJobStore(url='postgresql://postgres:postgres@localhost:5432/postgres')

scheduler = BackgroundScheduler(jobstores={'default': jobstore})
scheduler.start()

@dramatiq.actor(max_retries=0, min_backoff=0, max_backoff=0)
def count_words(url):
    response = requests.get(url)
    time.sleep(400)
    count = len(response.text.split(" "))
    print(f"There are {count} words at {url!r}.")

@dramatiq.actor(queue_name="rabbitmq")
def rabbitmq(url):
    response = requests.get(url)
    time.sleep(400)
    count = len(response.text.split(" "))
    print(f"There are {count} words at {url!r}.")

def schedule_job(url):
    print(url)
    return count_words.send(url)

def schedule2_job(url):
    print(url)
    return rabbitmq.send(url)

@app.get("/process_url")
def process_url(hour: int = 13, minute: int = 10):
    url = 'http://example.com'
    res = scheduler.add_job(schedule_job, CronTrigger(hour=hour, minute=minute, second=0), args=(url,))
    print('Job has been added.')
    print(res.id)
    return {"message": "processing has been started."}

@app.get("/rabbit_url")
def process_url(hour: int = 13, minute: int = 10):
    url = 'https://rabbitmq.com'
    res = scheduler.add_job(schedule2_job, CronTrigger(hour=hour, minute=minute, second=0), args=(url,))
    print('Job has been added.')
    print(res.id)
    return {"message": "processing has been started."}


@app.post("/pause_job/{job_id}")
def pause_job(job_id: str):
    res = scheduler.pause_job(job_id)
    print(res)
    return {"message": f"Job {job_id} has been paused."}


@app.post("/resume_job/{job_id}")
def resume_job(job_id: str):
    res = scheduler.resume_job(job_id)
    print(res)
    return {"message": f"Job {job_id} has been resumed."}

if __name__ == "__main__":
    import uvicorn
    print("Started..")
    uvicorn.run(app, host="localhost", port=8000)
