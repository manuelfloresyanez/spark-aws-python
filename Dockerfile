FROM bde2020/spark-python-template:3.2.0-hadoop3.2
COPY . .
CMD ["sh", "-c", "python3 -m PYTHON_ML_AWS_SDK"]
