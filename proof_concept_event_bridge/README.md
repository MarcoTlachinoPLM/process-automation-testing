

####  Proof Concept EventBridge  ###
1. Empaquetar dependencias y subir como capa:

cd proof_concept_event_bridge

mkdir package

2. Cree un paquete para funcion Lambda.

# 2.1 Generar Lambda Package
cd lambda_function

zip -r ../lambda_package.zip .

3. Crear archivo zip para Layer y actualizarlo en AWS.

cd pyodbc_layer

# 3.1 Construye la imagen Docker

docker build --platform linux/arm64 -t pyodbc-builder-arm64 .

# docker build -t pyodbc-builder .

# 3.2 Create a temporary container

docker create --name temp lambda-pyodbc-builder

# 3.3 Copy the zip file from the container

docker cp temp:pyodbc-layer.zip .

docker rm temp

unzip -l pyodbc-layer.zip

unzip -l pyodbc-layer.zip | grep pyodbc.cpython

# 3.4 Publicar la nueva versión de la capa

aws lambda publish-layer-version \
    --layer-name pyodbc-sqlserver-layer \
    --description "Layer ARM64 para pyodbc construido desde Mac M1. Incluye: pyodbc 4.0.39, ODBC Driver 18, configurado para Lambda Python 3.11 ARM..." \
    --zip-file fileb://pyodbc-layer.zip \
    --compatible-runtimes python3.11 \
    --compatible-architectures arm64

###  End Proof Concept EventBridge  ###

