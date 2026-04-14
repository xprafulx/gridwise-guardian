#!/bin/bash

echo "🚀 Firing up the Greenhour Guardian..."
docker run -p 8501:8501 --env-file .env greenhour-guardian