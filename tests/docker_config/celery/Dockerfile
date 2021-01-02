FROM python:3.8-slim as compile-image

RUN apt-get update && apt-get install -y --no-install-recommends gcc build-essential libpq-dev git

RUN mkdir -m 777 /app
COPY ./requirements.txt /
COPY ./app /app
WORKDIR /venv
RUN python -m venv .
RUN /venv/bin/pip install --upgrade pip && /venv/bin/pip install --no-cache-dir -r /requirements.txt


FROM python:3.8-slim AS runtime-image

RUN apt-get update && apt-get install -y  --no-install-recommends netcat

COPY --from=compile-image /app /app
COPY --from=compile-image /venv /venv

COPY ./docker_config/celery/start.sh /start.sh
RUN sed -i 's/\r//' /start.sh
RUN chmod +x /start.sh

ENV PATH="/venv/bin:$PATH"

WORKDIR /app

CMD ["/start.sh"]
