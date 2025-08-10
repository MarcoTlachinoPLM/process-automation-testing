# process-automation-testing
Automatizacion de procesos, pruebas de concepto...


1. Instalar el driver ODBC de SQL Server (msodbcsql17) en macOS

# 1.1. Instalar unixODBC

brew install unixodbc

# 1.2.  Agregar tap de Microsoft y actualizar fórmulas

brew tap microsoft/mssql-release https://github.com/Microsoft/homebrew-mssql-release
brew update

# 1.3. Instalar el driver msodbcsql17
ACCEPT_EULA=Y brew install msodbcsql17

# 1.4 Verificar instalación
odbcinst -q -d -n "ODBC Driver 17 for SQL Server"


2. Configurar credenciales de AWS

# 2.1 Validar version aws cli
aws --version

# 2.2 Si no encuentra aws cli
brew install awscli

# 2.3 Configurar las credenciales
aws configure

# Te pedirá la siguiente información:
# AWS Access Key ID: Tu clave de acceso de AWS
# AWS Secret Access Key: Tu clave secreta
# Default region name: La región AWS que prefieras (us-east-1, eu-west-1, etc...)
# Default output format: El formato de salida (json, text, table)

# 2.4 Verificar la configuración
aws sts get-caller-identity

# 2.5 Configurar variables de entorno (Opcional)
export AWS_ACCESS_KEY_ID=TU_ACCESS_KEY
export AWS_SECRET_ACCESS_KEY=TU_SECRET_KEY
export AWS_DEFAULT_REGION=us-east-1



####  Proof Concept Pyodbc  ###
1. Instale las bibliotecas externas en un nuevo directorio package.

cd prcss_export_rds_to_s3

# 1.1 Crear un entorno virtual

python3 -m venv venv

# 1.2 Activar el entorno virtual

source venv/bin/activate

pip install pyodbc boto3

2. Ejecutar prueba.

python export_rds_to_s3.py

###  End Proof Concept Pyodbc  ###



####  Proof Concept Import S3 to RDS  ###
1. Instale las bibliotecas externas en un nuevo directorio package.

cd prcss_import_s3_to_rds

# 1.1 Crear un entorno virtual

rm -rf venv

python3 -m venv venv

# 1.2 Activar el entorno virtual

source venv/bin/activate

which pip

pip list

pip install pyodbc boto3 beautifulsoup4

pip install --upgrade pip

2. Ejecutar prueba.

python trnsfrm_html_to_json.py

python prcss_import_s3_to_rds/import_html_to_rds_v1.py

python import_html_to_rds_v2.py

python import_s3_to_rds.py

python prcss_import_s3_to_rds/import_files_to_rds.py

###  End Proof Concept Import S3 to RDS  ###



####  Proof Concept Import CSV to RDS  ###
1. Instale las bibliotecas externas en un nuevo directorio package.

cd prcss_import_csv_to_rds

# 1.1 Crear un entorno virtual

rm -rf venv

python3 -m venv venv

# 1.2 Activar el entorno virtual

source venv/bin/activate

which pip

pip list

pip install pyodbc boto3 pandas beautifulsoup4

pip install --upgrade pip

2. Ejecutar prueba.

python prcss_import_csv_to_rds/import_csv_to_rds_v1.py

python prcss_import_csv_to_rds/import_csv_to_rds_v2.py

###  End Proof Concept Import CSV to RDS  ###



####  AWS Lambda layer pyodbc  ###
1. Empaquetar dependencias y subir como capa:
# Consola Linux
mkdir -p aws_pyodbc_layer/python/lib/python3.11/site-packages

# Con Dockerfile
mkdir aws_pyodbc_layer

cd aws_pyodbc_layer

### Init Consola Linux ###
# 1.1 Instalar dependencias en la ubicación correcta
pip3 install --target ./python/lib/python3.11/site-packages pyodbc boto3

# 1.2 Descargar e incluir las dependencias nativas de ODBC
# Necesitarás Docker para este paso
docker pull amazonlinux:2
docker run -v $(pwd):/output -it amazonlinux:2 bash -c "
    yum install -y gcc-c++ unixODBC-devel tar gzip findutils &&
    cp /usr/lib64/libltdl.so.7 /output/python/ &&
    cp /usr/lib64/libodbc.so.2 /output/python/ &&
    cp /usr/lib64/libodbcinst.so.2 /output/python/
"

# 1.2 Incluir librerías nativas (ODBC)
docker run -v $(pwd):/output -it amazonlinux:2 bash -c \
    "yum install -y unixODBC-devel && \
    cp /usr/lib64/libltdl.so.7 /output/python/ && \
    cp /usr/lib64/libodbc.so.2 /output/python/ && \
    cp /usr/lib64/libodbcinst.so.2 /output/python/"

# 1.3 Empaquetar
zip -r pyodbc-layer-aws.zip python
### End Consola Linux ###

### Init Dockerfile ###
# 1.1 Construye la imagen Docker

docker build -t lambda-pyodbc-builder .

# Extract the layer (using Method 1)
# docker run --rm -v $(pwd):/output -it lambda-pyodbc-builder bash -c "cp /pyodbc-layer.zip /output/"

# 1.2 Create a temporary container
docker create --name temp lambda-pyodbc-builder

# 1.3 Copy the zip file from the container
docker cp temp:pyodbc-layer.zip .

docker rm temp

unzip -l pyodbc-layer.zip

unzip -l pyodbc-layer.zip | grep pyodbc.cpython

### End Dockerfile ###

# 1.4 Publicar la nueva versión de la capa

aws lambda publish-layer-version \
    --layer-name pyodbc-layer \
    --zip-file fileb://pyodbc-layer.zip \
    --compatible-runtimes python3.11

# 1.5 Lambda Configuration add:

LD_LIBRARY_PATH=/opt/python:/opt/lib
ODBCSYSINI=/opt
ODBCINI=/opt/odbc.ini

# NOTA: Si no funciona, optar por solucion Alternativa

2. Solución Alternativa: Usar una Capa Preconstruida

aws iam list-attached-user-policies --user-name marco.tlachino@plmlatina.com

arn:aws:lambda:us-east-1:225733257624:layer:pyodbc:1

###  End AWS Lambda layer pyodbc  ###



####  Docker layer pyodbc  ###
1. Instale las bibliotecas externas en un nuevo directorio package.

cd docker-pyodbc-layer

# 1.1 Reconstruye la Imagen Docker

docker buildx build --platform linux/amd64 -t pyodbc-lambda-layer-x86 .

# 1.2. Ejecuta un Contenedor Temporal para Copiar el ZIP

CONTAINER_ID=$(docker run -d --platform linux/amd64 pyodbc-lambda-layer-x86 sleep 5)

docker cp "$CONTAINER_ID":/tmp/pyodbc_layer.zip ./pyodbc_layer_x86.zip

docker rm -f "$CONTAINER_ID"

# NOTA: Mantiene el contenedor vivo durante 5 segundos. Es un pequeño truco para asegurarnos de que docker cp tenga tiempo de conectarse y el archivo esté "listo".

# 1.2. Verifica la existencia del archivo dentro del Contenedor

docker run --rm -it --platform linux/amd64 --entrypoint /bin/bash pyodbc-lambda-layer-x86

ls -lh /tmp/pyodbc_layer.zip

exit

# 5. Obtén el ID real del contenedor
docker images

ls -lh pyodbc_layer_x86.zip

docker cp <ID_CONTAINER>:/tmp/pyodbc_layer.zip ./pyodbc_layer_x86.zip


###  End Docker layer pyodbc  ###



####  Docker layer pyodbc V2  ###
1. Instale las bibliotecas externas en un nuevo directorio package.

cd docker-pyodbc-layer-v2

# 1.1 Reconstruye la Imagen Docker

docker buildx build --platform linux/amd64 -t pyodbc-lambda-layer-x86 .

# 1.2. Ejecuta un Contenedor Temporal para Copiar el ZIP

CONTAINER_ID=$(docker run -d --platform linux/amd64 pyodbc-lambda-layer-x86 sleep 5)

docker cp "$CONTAINER_ID":/tmp/pyodbc_layer.zip ./pyodbc_layer_x86.zip

docker rm -f "$CONTAINER_ID"

# NOTA: Mantiene el contenedor vivo durante 5 segundos. Es un pequeño truco para asegurarnos de que docker cp tenga tiempo de conectarse y el archivo esté "listo".

# 1.2. Verifica la existencia del archivo dentro del Contenedor

docker run --rm -it --platform linux/amd64 --entrypoint /bin/bash pyodbc-lambda-layer-x86

ls -lh /tmp/pyodbc_layer.zip

exit

# 5. Obtén el ID real del contenedor
docker images

ls -lh pyodbc_layer_x86.zip

docker cp <ID_CONTAINER>:/tmp/pyodbc_layer.zip ./pyodbc_layer_x86.zip


###  End Docker layer pyodbc V2  ###

