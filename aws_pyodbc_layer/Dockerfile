FROM public.ecr.aws/lambda/python:3.11 AS builder

# Install all dependencies
RUN yum install -y gcc-c++ unixODBC-devel zip unzip openssl-devel && \
    yum clean all

# Install Microsoft ODBC Driver
RUN curl https://packages.microsoft.com/config/rhel/9/prod.repo > /etc/yum.repos.d/mssql-release.repo && \
    ACCEPT_EULA=Y yum install -y msodbcsql18 && \
    yum clean all

# Install pyodbc with specific flags
RUN pip install pyodbc==4.0.39 --no-binary pyodbc -t /output/python/lib/python3.11/site-packages

# Copy all required files
RUN cp -r /usr/lib64/*odbc* /output/python/ && \
    cp /usr/lib64/libltdl.so.7 /output/python/ && \
    cp -r /opt/microsoft /output/ && \
    cp /etc/odbcinst.ini /output/

# Create configuration
RUN echo -e "[ODBC Driver 18 for SQL Server]\n\
Driver=/opt/microsoft/msodbcsql18/lib64/libmsodbcsql-18.1.so.1.1\n\
UsageCount=1" > /output/odbc.ini

# Create zip
RUN cd /output && zip -r /pyodbc-layer.zip *
