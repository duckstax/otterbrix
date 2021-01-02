from typing import Generator, AsyncGenerator
import wave
import math
import asyncio
import requests
import io
import struct
import subprocess

from taskapp.celery import celery_app


def get_request(url: str) -> requests.Response:
    return requests.get(url, timeout=3)


def create_generator() -> Generator:
    for i in range(10):
        yield i * i


async def async_create_generator() -> AsyncGenerator:
    for i in range(10):
        yield i * i
        await asyncio.sleep(0.1)


async def read_from_async_gen() -> int:
    s = 0
    async for i in async_create_generator():
        s += i

    return s


@celery_app.task(bind=True, name="subprocess_task")
def subprocess_task(self, *args, **kwargs):
    command = "ls"

    process_handle = subprocess.Popen(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        shell=True,
    )
    process_handle.communicate()
    status = process_handle.returncode

    assert not status


@celery_app.task(bind=True, name="create_audio_wave_task")
def create_audio_wave_task(self, *args, **kwargs):
    sample_rate = 44100.0
    duration = 500
    nchannels = 1
    sampwidth = 2

    comptype = "NONE"
    compname = "not compressed"

    volume = 1.0
    freq = 440.0
    audio = []
    num_samples = duration * (sample_rate / 1000.0)
    for x in range(int(num_samples)):
        audio.append(volume * math.sin(2 * math.pi * freq * (x / sample_rate)))

    nframes = len(audio)

    ok: bool = True
    try:
        with io.BytesIO() as buf:
            with wave.open(buf, "w") as wav_file:
                wav_file.setparams(
                    (nchannels, sampwidth, sample_rate, nframes, comptype, compname)
                )
                for sample in audio:
                    wav_file.writeframes(struct.pack("h", int(sample * 32767.0)))

            buf.seek(0)
    except Exception as er:
        print(er)
        ok = False

    assert ok


@celery_app.task(bind=True, name="async_task")
def async_task(self, *args, **kwargs):
    async def coro() -> int:
        await asyncio.sleep(1)
        return 1

    res = asyncio.run(coro())

    assert res == 1


@celery_app.task(bind=True, name="request_task")
def request_task(self, *args, **kwargs):
    url = "https://google.com/"

    try:
        resp = get_request(url)
        code = resp.status_code
    except requests.RequestException:
        code = 500

    assert 500 > code


@celery_app.task(bind=True, name="yield_task")
def yield_task(self, *args, **kwargs):
    assert 285 == sum(create_generator())


@celery_app.task(bind=True, name="async_yield_task")
def async_yield_task(self, *args, **kwargs):
    res = asyncio.run(read_from_async_gen())
    assert 285 == res


@celery_app.task(bind=True, name="start_many_tasks")
def start_many_tasks(self, *args, **kwargs):
    for _ in range(1000):
        subprocess_task.delay()
