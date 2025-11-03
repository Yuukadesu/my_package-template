FROM python:3.13-slim

WORKDIR /function

COPY main.py /function/main.py

CMD ["python", "/function/main.py"]