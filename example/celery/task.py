import logging
from celery import Celery
from celery.schedules import crontab

app = Celery('tasks', backend='redis://localhost', broker='pyamqp://')

app.conf.update(
    task_serializer='json',
    accept_content=['json'],  # Ignore other content
    result_serializer='json',
    timezone='Europe/Oslo',
    enable_utc=True,
)

app.conf.beat_schedule = {
    'add-every-monday-morning': {
        'task': 'tasks.add',
        'schedule': crontab(hour=7, minute=30, day_of_week=1),
        'args': (16, 16),
    },
}


# app.config_from_object('celeryconfig')

@app.task
def add(x, y):
    return x + y


@app.task(ignore_result=True)
def print_hello():
    print('hello there')


@app.task(serializer='json')
def create_user(username, password):
    pass
    # big work in db


@app.task(bind=True)
def add(self, x, y):
    pass
    # write log

