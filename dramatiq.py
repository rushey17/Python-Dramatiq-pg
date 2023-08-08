import dramatiq
import sys
import requests
from dramatiq_pg import PostgresBroker
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger
from dramatiq_pg import PostgresBackend
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from datetime import datetime
import os
from dramatiq.results import Results

# from dramatiq.brokers.stub import StubBroker
from periodiq import cron, PeriodiqMiddleware
broker = PostgresBroker(url="postgresql://postgres:postgres@localhost:5432/postgres")
backend = PostgresBackend(url="postgresql://postgres:postgres@localhost:5432/postgres")
broker.add_middleware(PeriodiqMiddleware())
broker.add_middleware(Results(backend=backend, result_ttl=3600*1000))

dramatiq.set_broker(broker)

@dramatiq.actor(store_results=True)
def count_words():
    url = "http://example.com"
    response = requests.get(url)
    count = len(response.text.split(" "))
    print(f"There are {count} words at {url!r}.")
    return 'Hello world'

if __name__ == '__main__':
   scheduler = BlockingScheduler()
   scheduler.start()
   scheduler.add_job(
      count_words.send,
      'interval', 
      minutes=1
   )
