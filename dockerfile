FROM python:3.11

# install java + dependencies
RUN apt-get update && apt-get install -y \
    openjdk-11-jdk \
    curl \
    unzip \
    tar \
    && rm -rf /var/lib/apt/lists/*

# java env vars
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# work directory
WORKDIR /app

# copy project files
COPY src/ ./src/
COPY requirements.txt .

# install dependencies
RUN pip3 install --no-cache-dir -r requirements.txt

# create output and input dirs
RUN mkdir -p /app/data/input /app/data/output

# etl execution
CMD ["python3", "src/main.py", "--input_path", "/app/data/input", "--output_path", "/app/data/output"]
