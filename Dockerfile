FROM python:3.10-slim

# FFmpeg install karo (IMPORTANT!)
RUN apt-get update && apt-get install -y ffmpeg && apt-get clean

WORKDIR /app

# Requirements copy aur install karo
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Saari files copy karo
COPY . .

# Bot start karo
CMD ["python", "PREMIUM.py"]
