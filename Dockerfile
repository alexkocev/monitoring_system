FROM python:3.9-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the application code
COPY main.py .

# Copy the credentials files
COPY sandbox-a-451617-e3eaf9bc7d8b.json .
COPY steadfast-bebop-389314-c6da75a035db.json .

# Run the script
CMD ["python", "main.py"]