# Lambda SQL Server to S3 (Docker-based)

Esta función Lambda conecta a una base de datos SQL Server en RDS, extrae datos, los convierte a JSON y los sube a un bucket S3. Funciona con `pyodbc` gracias a una imagen Docker personalizada.

## 🧱 Requisitos

- Cuenta AWS
- Acceso a ECR
- SQL Server (RDS o externo)
- Bucket S3

## ⚙️ Variables de entorno Lambda

Configura estas variables en la función Lambda:

```env
DB_SERVER=your-db-endpoint.rds.amazonaws.com
DB_NAME=your_db
DB_USER=your_user
DB_PASSWORD=your_pass
S3_BUCKET=your-bucket-name

docker buildx build --platform linux/amd64 -t lambda-sqlserver .
