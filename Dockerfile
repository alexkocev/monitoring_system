FROM python:3.11-slim

WORKDIR /app

# Copy requirements first for better caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the application code
COPY main.py .

# Copy all JSON files - including service-account-key.json and BQ key
COPY *.json ./

# Create a directory for output images
RUN mkdir -p images

# Run the script
CMD ["python", "main.py"]