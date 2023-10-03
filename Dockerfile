FROM python:3.8
RUN pip install requests  
RUN pip install pandas
RUN pip install python-decouple
RUN pip install sqlalchemy


WORKDIR /app
COPY Entregable.py .

CMD ["python", "Entregable.py"]