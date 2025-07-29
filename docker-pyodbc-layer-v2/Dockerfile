FROM amazonlinux:2

# 1. Instalar herramientas necesarias
RUN yum update -y && \
    yum install -y \
    gcc gcc-c++ \
    python3 python3-devel \
    unixODBC unixODBC-devel \
    libtool-ltdl \
    wget curl zip tar gzip

# 2. Descargar RPMs manualmente (desde mirrors válidos)
WORKDIR /tmp

RUN curl -O https://packages.microsoft.com/rhel/7/prod/msodbcsql17-17.10.4.1-1.x86_64.rpm && \
    curl -O https://packages.microsoft.com/rhel/7/prod/msodbcsql-tools17-17.10.4.1-1.x86_64.rpm

# 3. Instalar msodbcsql17 manualmente
RUN ACCEPT_EULA=Y yum localinstall -y msodbcsql17-17.10.4.1-1.x86_64.rpm && \
    yum clean all

# 4. Crear carpeta de la Layer
ENV LAYER_DIR=/opt/layer
RUN mkdir -p ${LAYER_DIR}/python

# 5. Instalar paquetes de Python
RUN pip3 install --upgrade pip && \
    pip3 install pyodbc boto3 --target ${LAYER_DIR}/python

# 6. Copiar librerías nativas necesarias
RUN mkdir -p ${LAYER_DIR}/python/lib && \
    mkdir -p ${LAYER_DIR}/python/lib64 && \
    cp -v /usr/lib64/libodbc.so* ${LAYER_DIR}/python/lib64/ && \
    cp -v /usr/lib64/libltdl.so* ${LAYER_DIR}/python/lib64/ && \
    cp -v /opt/microsoft/msodbcsql17/lib64/libmsodbcsql-17.* ${LAYER_DIR}/python/lib64/

# 7. Crear el ZIP
RUN cd ${LAYER_DIR} && zip -r /pyodbc-layer.zip .
