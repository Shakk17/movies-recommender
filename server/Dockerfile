# Pull official base image.
FROM python:3.7

# Set work directory.
WORKDIR /code

# Set environmental variables.
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONBUFFERED 1
ENV DEBUG 1

# Copy the dependencies file to the working directory.
COPY requirements.txt .

# Install dependencies.
RUN pip install -r requirements.txt

# Copy the content of the local src directory to the working directory.
COPY src .

# FOR HEROKU: add and run as non-root user. (if alpine)
#RUN adduser -D myuser
#USER myuser

# Command to run on container start.
# CMD ["python", "./run_crawler.py"]