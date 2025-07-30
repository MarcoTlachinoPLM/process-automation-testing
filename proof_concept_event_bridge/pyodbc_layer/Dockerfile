FROM --platform=linux/arm64 public.ecr.aws/lambda/python:3.11

# Instalar dependencias del sistema
RUN yum install -y gcc-c++ unixODBC-devel unixODBC zip unzip openssl-devel && \
    yum clean all

# Instalar Microsoft ODBC Driver (versión ARM64)
RUN curl https://packages.microsoft.com/config/rhel/9/prod.repo > /etc/yum.repos.d/mssql-release.repo && \
    ACCEPT_EULA=Y yum install -y msodbcsql18 && \
    yum clean all

# Configuración específica para ARM64
RUN mkdir -p /tmp/layer/python/lib/python3.11/site-packages && \
    mkdir -p /tmp/layer/lib

# Instalación optimizada para ARM64
RUN pip install pyodbc==4.0.39 --no-binary pyodbc -t /tmp/layer/python/lib/python3.11/site-packages

# Copiar bibliotecas específicas para ARM64
RUN cp -r /usr/lib64/*odbc* /tmp/layer/lib/ && \
    cp /usr/lib64/libltdl.so.7 /tmp/layer/lib/ && \
    cp -r /opt/microsoft/msodbcsql18/lib64/ /tmp/layer/lib/ && \
    cp /etc/odbcinst.ini /tmp/layer/

# Configuración ODBC para ARM64
RUN echo -e "[ODBC Driver 18 for SQL Server]\n\
Driver=/opt/lib/libmsodbcsql-18.1.so.1.1\n\
UsageCount=1" > /tmp/layer/odbc.ini

# Script de inicialización para ARM64
RUN echo -e 'import os\n\
os.environ["LD_LIBRARY_PATH"] = "/opt/lib:/opt/python/lib/python3.11/site-packages"\n\
os.environ["ODBCINI"] = "/opt/odbc.ini"\n\
os.environ["ODBCSYSINI"] = "/opt"' > /tmp/layer/python/lib/python3.11/site-packages/layer_init.py

# Empaquetado final
WORKDIR /tmp/layer
RUN zip -r /tmp/pyodbc-layer.zip .
