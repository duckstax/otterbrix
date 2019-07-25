from pyrocketjoe.celery import Celery

app = Celery()

@app.task()
def simple_task() -> int:
    return 42

job = simple_task()

print(job.get())
