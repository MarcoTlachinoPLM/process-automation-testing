# Usa una imagen base que simule el entorno de Lambda para x86_64
FROM public.ecr.aws/lambda/python:3.11

# Establece el directorio de trabajo dentro del contenedor
WORKDIR /tmp

# Instalar pyodbc en el formato requerido por las capas Lambda
RUN pip install pyodbc -t python/

# --- Instalación del controlador ODBC para SQL Server ---
# Descargar el RPM del controlador (versión 17 para RHEL 7 / Amazon Linux 2)
RUN curl -O https://packages.microsoft.com/rhel/7/prod/msodbcsql17-17.10.1.1-1.x86_64.rpm && \
    rpm2cpio msodbcsql17-17.10.1.1-1.x86_64.rpm | cpio -idmv && \
    mkdir -p lib && \
    mv opt/microsoft/msodbcsql17/lib64/* lib/

# --- ¡NUEVAS LÍNEAS AQUÍ! ---
# Instalar unixODBC y sus librerías de desarrollo
# Luego, copiar las librerías compartidas de unixODBC al directorio 'lib/'
# Asegúrate de que las versiones de yum y las rutas de librerías son correctas para Amazon Linux
RUN yum install -y unixODBC unixODBC-devel && \
    cp /usr/lib64/libodbc.so.2 lib/ && \
    cp /usr/lib64/libodbcinst.so.2 lib/

# Instalar la utilidad 'zip'
RUN yum install -y zip

# Comprimir la capa: ambos directorios 'python/' y 'lib/' deben estar en el zip.
RUN zip -r pyodbc_layer.zip python/ lib/

# El CMD puede ser algo para mantener el contenedor vivo si lo ejecutas sin comando,
# pero para la construcción del zip, no es crucial.
CMD ["tail", "-f", "/dev/null"]
