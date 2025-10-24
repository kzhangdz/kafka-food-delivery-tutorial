# guide on dockerizing python applications
# https://www.docker.com/blog/how-to-dockerize-your-python-applications/

# python with docker-compose
# https://www.cloudbees.com/blog/using-docker-compose-for-python-development

# Use an official Python runtime as a parent image
FROM python:3.11-slim

# mkdir statement not needed bc WORKDIR will create the dir
# mkdir sometimes necessary bc WORKDIR does not respect the USER command
# RUN mkdir /usr/src/app
WORKDIR /usr/src/app

# Copy the requirements file and install dependencies
# Always specify a version for requirements.
# The idea is to keep a consistent version between dev and prod,
# Which 
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# set this environment variable to view logging
ENV PYTHONUNBUFFERED 1

# Copy the rest of the application code to the working directory
COPY . .

CMD ["python", "-u", "producer.py"]
# run the application by doing docker run python-imagename
