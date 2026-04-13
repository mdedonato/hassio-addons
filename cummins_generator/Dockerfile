ARG BUILD_FROM
FROM $BUILD_FROM

ENV LANG C.UTF-8

WORKDIR /app

COPY requirements.txt .
RUN apk add --no-cache python3 py3-pip && \
    pip3 install --no-cache-dir --break-system-packages -r requirements.txt

COPY cummins.py .
COPY config.ini .

COPY run.sh /
RUN chmod a+x /run.sh

CMD [ "/run.sh" ]
