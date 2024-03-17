FROM python:3.11-slim

RUN mkdir "/base"
COPY requirements.txt /apps/requirements.txt
COPY requirements_dev.txt /apps/requirements_dev.txt

RUN pip install -r /apps/requirements.txt
RUN pip install -r /apps/requirements_dev.txt

COPY parrot_integrations/ /base/parrot_integrations/
COPY test.sh /base/test.sh

RUN chmod +x /base/test.sh

WORKDIR /base

