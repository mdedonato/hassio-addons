ARG BUILD_FROM
FROM $BUILD_FROM

ENV LANG C.UTF-8

FROM python:3
ADD cummins.py /
COPY requirements.txt /
COPY config.ini /

#CMD [ "python", "./cummins.py" ]
# Copy data for add-on
COPY run.sh /
RUN chmod a+x /run.sh
RUN pip install -r requirements.txt
CMD [ "/run.sh" ]
