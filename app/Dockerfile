FROM python:3.12

WORKDIR /app/

COPY app_requirements.txt .

RUN pip install -r app_requirements.txt

COPY . .

ENTRYPOINT ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8080"]