# Using Python 3.10 slim base image for efficiency
FROM python:3.10-slim

# Set working directory
WORKDIR /app

# Copy requirements file and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the entire project
COPY . .

# Command to keep the container running (overridden by docker-compose)
CMD ["tail", "-f", "/dev/null"]