FROM python:3.10
WORKDIR /app
COPY fetch_news.py requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
CMD ["python", "fetch_news.py"]
