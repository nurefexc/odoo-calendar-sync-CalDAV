FROM python:3.11-slim

WORKDIR /app

COPY app/requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY app/main.py ./
COPY app/odoo_connector.py ./

ENV TZ=UTC

CMD ["python", "main.py"]

