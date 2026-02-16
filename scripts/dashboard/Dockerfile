FROM python:3.11-slim

# Set working directory inside container
WORKDIR /app

# Copy only requirements first (for caching)
COPY requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt
RUN pip install psycopg2-binary

# Copy the dashboard code
COPY . .

# Expose Dash default port
EXPOSE 8050

# Run the dashboard
CMD ["python", "dashboard_app.py"]