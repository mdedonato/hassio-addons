ARG BUILD_FROM
FROM $BUILD_FROM

ENV LANG C.UTF-8

FROM python:3
RUN git clone https://github.com/mdedonato/cummins_generator.git
ADD cummins_generator/cummins.py /
COPY cummins_generator/requirements.txt /
COPY cummins_generator/config.ini /
RUN pip install -r requirements.txt

#CMD [ "python", "./cummins.py" ]
# Copy data for add-on
COPY run.sh /
RUN chmod a+x /run.sh

CMD [ "/run.sh" ]
