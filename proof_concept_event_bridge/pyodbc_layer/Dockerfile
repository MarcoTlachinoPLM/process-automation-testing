FROM public.ecr.aws/lambda/python:3.11

# Instalar dependencias del sistema
RUN yum install -y gcc-c++ unixODBC-devel unixODBC zip unzip openssl-devel && \
    yum clean all

# Instalar Microsoft ODBC Driver
RUN curl https://packages.microsoft.com/config/rhel/9/prod.repo > /etc/yum.repos.d/mssql-release.repo && \
    ACCEPT_EULA=Y yum install -y msodbcsql18 && \
    yum clean all

# Crear estructura de directorios para el layer
RUN mkdir -p /tmp/layer/python/lib/python3.11/site-packages && \
    mkdir -p /tmp/layer/lib

# Instalar pyodbc específicamente para Lambda
RUN pip install pyodbc==4.0.39 --no-binary pyodbc -t /tmp/layer/python/lib/python3.11/site-packages

# Copiar bibliotecas necesarias
RUN cp -r /usr/lib64/*odbc* /tmp/layer/lib/ && \
    cp /usr/lib64/libltdl.so.7 /tmp/layer/lib/ && \
    cp -r /opt/microsoft /tmp/layer/ && \
    cp /etc/odbcinst.ini /tmp/layer/

# Crear archivo de configuración ODBC
RUN echo -e "[ODBC Driver 18 for SQL Server]\n\
Driver=/opt/microsoft/msodbcsql18/lib64/libmsodbcsql-18.1.so.1.1\n\
UsageCount=1" > /tmp/layer/odbc.ini

# Crear script de inicialización para configurar variables de entorno
RUN echo -e 'import os\n\
os.environ["LD_LIBRARY_PATH"] = "/opt/lib:/opt/python/lib/python3.11/site-packages"\n\
os.environ["ODBCINI"] = "/opt/odbc.ini"\n\
os.environ["ODBCSYSINI"] = "/opt"' > /tmp/layer/python/lib/python3.11/site-packages/layer_init.py

# Comprimir el layer
WORKDIR /tmp/layer
RUN zip -r /tmp/pyodbc-layer.zip .
