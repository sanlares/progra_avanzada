FROM python:3.8

WORKDIR pipeline

COPY requirements.txt .

RUN pip install -r requirements.txt

ENTRYPOINT ["python", "pipeline.py"]