FROM public.ecr.aws/lambda/python:3.13 AS builder

ENV ODBCINI=/opt/odbc.ini
ENV ODBCSYSINI=/opt/
ARG UNIXODBC_VERSION=2.3.12

RUN dnf install -y gzip tar openssl-devel gcc gcc-c++ make automake kernel-devel

RUN curl ftp://ftp.unixodbc.org/pub/unixODBC/unixODBC-${UNIXODBC_VERSION}.tar.gz -O \
    && tar xzvf unixODBC-${UNIXODBC_VERSION}.tar.gz \
    && cd unixODBC-${UNIXODBC_VERSION} \
    && ./configure --sysconfdir=/opt --disable-gui --disable-drivers --enable-iconv --with-iconv-char-enc=UTF8 --with-iconv-ucode-enc=UTF16LE --prefix=/opt \
    && make \
    && make install

RUN curl https://packages.microsoft.com/config/rhel/9/prod.repo > /etc/yum.repos.d/mssql-release.repo
RUN dnf install -y e2fsprogs fuse-libs libss
RUN ACCEPT_EULA=Y dnf install -y msodbcsql18

ENV CFLAGS="-I/opt/include"
ENV LDFLAGS="-L/opt/lib"

RUN mkdir -p /layer/python/lib/python3.13/site-packages \
    && pip install pyodbc -t /layer/python/lib/python3.13/site-packages \
    && cp /opt/lib/*.so* /layer/python/

# Create the zip file in a known location
RUN cd /layer && zip -r /pyodbc-layer.zip python

FROM public.ecr.aws/lambda/python:3.13

RUN dnf install -y openssl

COPY --from=builder /opt/microsoft /opt/microsoft
COPY --from=builder /opt/lib /opt/lib

# This is just to make the zip file available for copying
COPY --from=builder /pyodbc-layer.zip /
