import asyncio
from pyrocketjoe.celery import Celery
from pyrocketjoe.celery.apps import Worker


app = Celery()
worker = Worker(app)


@app.task()
def hello_world(a: int, b: int) -> None:
    print('Hello, World!')

    return a + b


result = hello_world.apply_async(40, 2)


async def main() -> None:
    await worker


asyncio.run(main())
print(result.get())
