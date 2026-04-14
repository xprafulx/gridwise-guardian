# 1. Use an official Python runtime as a parent image
FROM python:3.11-slim

# 2. Set the working directory to the project root
WORKDIR /app

# 3. Install system dependencies for PostgreSQL and building tools
RUN apt-get update && apt-get install -y \
    build-essential \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# 4. Copy the requirements file into the container
COPY requirements.txt .

# 5. Install all dependencies from your final requirements list
RUN pip install --no-cache-dir -r requirements.txt

# 6. Copy the entire project structure (src folder, models, .env, etc.)
COPY . .

# 7. Set the PYTHONPATH so Python can find your 'src' module from anywhere
ENV PYTHONPATH="/app"

# 8. Expose the port Streamlit uses
EXPOSE 8501

# 9. Run the app, pointing to the specific subfolder path
CMD ["streamlit", "run", "src/frontend/app.py", "--server.port=8501", "--server.address=0.0.0.0"]