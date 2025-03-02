FROM python:3.11

# Instalar Java y dependencias
RUN apt-get update && apt-get install -y \
    openjdk-11-jdk \
    curl \
    unzip \
    tar \
    && rm -rf /var/lib/apt/lists/*

# Configurar variables de entorno para Java
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Establecer directorio de trabajo
WORKDIR /app

# Copiar archivos del proyecto
COPY src/ ./src/
COPY requirements.txt .

# Instalar dependencias de Python
RUN pip3 install --no-cache-dir -r requirements.txt

# Crear directorios para input y output
RUN mkdir -p /app/data/input /app/data/output

# Ejecutar el ETL con argumentos parametrizados
CMD ["python3", "src/main.py", "--input_path", "/app/data/input", "--output_path", "/app/data/output"]
