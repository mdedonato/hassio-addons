ARG BUILD_FROM
FROM $BUILD_FROM

ENV LANG C.UTF-8

FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
COPY cummins.py .
COPY config.ini .

RUN pip install --no-cache-dir -r requirements.txt

# Copy data for add-on
COPY run.sh /
RUN chmod a+x /run.sh

CMD [ "/bin/bash", "/run.sh" ]
