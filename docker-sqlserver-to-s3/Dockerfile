FROM amazonlinux:2

# Instalar dependencias base + RIE (Lambda runtime emulator)
RUN yum install -y \
    gcc gcc-c++ make python3 python3-pip unixODBC unixODBC-devel curl tar gzip shadow-utils \
 && curl https://packages.microsoft.com/keys/microsoft.asc | gpg --import \
 && curl -o /etc/yum.repos.d/mssql-release.repo https://packages.microsoft.com/config/rhel/7/prod.repo \
 && ACCEPT_EULA=Y yum install -y msodbcsql17 \
 && yum clean all

# Crear usuario Lambda
RUN useradd -m lambdauser

# Instalar dependencias Python
COPY requirements.txt .
RUN pip3 install --upgrade pip \
 && pip3 install -r requirements.txt

# Copiar código
COPY app.py .

# Copiar el Runtime Interface Emulator (solo para testing local)
ADD https://github.com/aws/aws-lambda-runtime-interface-emulator/releases/latest/download/aws-lambda-rie /usr/local/bin/aws-lambda-rie
RUN chmod +x /usr/local/bin/aws-lambda-rie

# Ejecutar como Lambda (entrypoint)
ENTRYPOINT ["/usr/local/bin/aws-lambda-rie", "python3", "-m", "awslambdaric"]
CMD ["app.lambda_handler"]
