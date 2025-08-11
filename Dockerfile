# Use official Python runtime as base image
FROM apify/actor-python:3.11

# Copy requirements and install dependencies
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY main.py ./

# Set the command to run Python script
CMD ["python", "main.py"]
