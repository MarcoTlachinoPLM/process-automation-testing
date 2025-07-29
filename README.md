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


2. onfigurar credenciales de AWS

export AWS_ACCESS_KEY_ID=TU_ACCESS_KEY
export AWS_SECRET_ACCESS_KEY=TU_SECRET_KEY



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

2. Ejecutar prueba.

python trnsfrm_html_to_json.py

python import_html_to_rds.py

python import_s3_to_rds.py

###  End Proof Concept Import S3 to RDS  ###



####  Proof Concept EventBridge  ###
1. Empaquetar dependencias y subir como capa:

cd proof_concept_event_bridge

pip3 install --target ./package boto3

pip3 install --target ./package pyodbc boto3

2. Cree un paquete de implementación con las bibliotecas instaladas.

cd package

zip -r ../lambda-sql-to-s3-package.zip .

cd ..

4. Agregue el archivo export_to_s3_with_aws_eb.py a la raíz del archivo zip.

zip lambda-sql-to-s3-package.zip export_to_s3_with_aws_eb.py

5. El archivo ZIP es el que se va a cargar en AWS Lambda

###  End Proof Concept EventBridge  ###



####  AWS Lambda layer pyodbc  ###
1. Empaquetar dependencias y subir como capa:
# mkdir -p aws_pyodbc_layer/python/lib/python3.11/site-packages

mkdir aws_pyodbc_layer

cd aws_pyodbc_layer

# 1.1 Instalar dependencias en la ubicación correcta
# pip3 install --target ./python/lib/python3.11/site-packages pyodbc boto3

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

# 1.1 Construye la imagen Docker

docker build -t lambda-pyodbc-builder .

# 1.2 Ejecuta el contenedor para crear la capa:

docker run -v $(pwd):/output -it lambda-pyodbc-builder bash -c \
    "mkdir -p python/lib/python3.11/site-packages && \
    /usr/local/bin/pip3.11 install --target python/lib/python3.11/site-packages pyodbc==4.0.39 && \
    cp /usr/lib64/libltdl.so.7 python/ && \
    cp /usr/lib64/libodbc.so.2 python/ && \
    cp /usr/lib64/libodbcinst.so.2 python/ && \
    zip -r /output/pyodbc-layer.zip python"

# 1.4 Publicar la nueva versión de la capa

aws lambda publish-layer-version \
    --layer-name pyodbc-layer \
    --zip-file fileb://pyodbc-layer-aws.zip \
    --compatible-runtimes python3.11

# NOTA: Si no funciona, optar por solucion Alternativa

2. Solución Alternativa: Usar una Capa Preconstruida

aws iam list-attached-user-policies --user-name marco.tlachino@plmlatina.com

arn:aws:lambda:us-east-1:225733257624:layer:pyodbc:1

arn:aws:lambda:us-east-1:225733257624:layer:testLayer:1


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



####  SQL Server Layer Environment  ###
1. Instale las bibliotecas externas en un nuevo directorio package.

cd sqlserver-layer_environment

# 1.1 Crear estructura del layer
mkdir -p lambda-layer/python/lib/python3.12/site-packages
cd lambda-layer

# 1.2. Instalar pyodbc dentro del path del layer
pip3 install pyodbc -t python/lib/python3.12/site-packages

# 3. Crear carpeta para librerías nativas
mkdir -p python/lib64

# 4. Copiar librerías ODBC y de SQL Server
cp /usr/lib64/libodbc*.so* python/lib64/
cp /opt/microsoft/msodbcsql17/lib64/libmsodbcsql-*.so python/lib64/
cp /opt/microsoft/msodbcsql17/lib64/libmsodbcsql17.so python/lib64/

# 5. Crear el ZIP final del layer
cd lambda-layer
zip -r9 pyodbc-layer.zip python


###  End SQL Server Layer Environment  ###

