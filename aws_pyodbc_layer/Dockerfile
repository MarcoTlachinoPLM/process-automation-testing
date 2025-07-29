FROM amazonlinux:2

# 1. Instala todas las dependencias necesarias
RUN yum install -y \
    gcc \
    gcc-c++ \
    make \
    openssl-devel \
    libffi-devel \
    bzip2-devel \
    xz-devel \
    zlib-devel \
    readline-devel \
    sqlite-devel \
    unixODBC-devel \
    tar \
    gzip \
    zip \
    wget \
    && yum clean all

# 2. Instala OpenSSL 1.1.1 desde source
RUN wget https://www.openssl.org/source/openssl-1.1.1w.tar.gz \
    && tar -xzf openssl-1.1.1w.tar.gz \
    && cd openssl-1.1.1w \
    && ./config --prefix=/usr/local/ssl --openssldir=/usr/local/ssl shared zlib \
    && make \
    && make install \
    && cd .. \
    && rm -rf openssl-1.1.1w*

# 3. Configura variables de entorno para SSL
ENV OPENSSL_ROOT_DIR=/usr/local/ssl
ENV LD_LIBRARY_PATH="/usr/local/ssl/lib:${LD_LIBRARY_PATH}"

# 4. Instala Python 3.11 con soporte SSL
RUN wget https://www.python.org/ftp/python/3.11.9/Python-3.11.9.tgz \
    && tar xzf Python-3.11.9.tgz \
    && cd Python-3.11.9 \
    && ./configure \
        --enable-optimizations \
        --with-openssl=/usr/local/ssl \
        --with-ssl-default-suites=openssl \
    && make altinstall \
    && cd .. \
    && rm -rf Python-3.11.9*

# 5. Instala pip usando el Python compilado
RUN curl -O https://bootstrap.pypa.io/get-pip.py \
    && /usr/local/bin/python3.11 get-pip.py \
    && rm get-pip.py

# 6. Configura el entorno de trabajo
WORKDIR /layer
ENV PATH="/usr/local/bin:${PATH}"

# 7. Comando para crear la capa Lambda
CMD mkdir -p python/lib/python3.11/site-packages && \
    /usr/local/bin/pip3.11 install --target python/lib/python3.11/site-packages pyodbc==4.0.39 && \
    cp /usr/lib64/libltdl.so.7 python/ && \
    cp /usr/lib64/libodbc.so.2 python/ && \
    cp /usr/lib64/libodbcinst.so.2 python/ && \
    zip -r /output/pyodbc-layer.zip python
