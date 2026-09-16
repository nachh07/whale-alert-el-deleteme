## whale-alert-el-deleteme
Proyecto de extraccion y carga de datos de https://whale-alert.io

## Entorno virtual

Crea un entorno virtual para instalar las dependencias del proyecto de forma
aislada:

```bash
python -m venv .venv
```

### Linux o macOS

```bash
source .venv/bin/activate
```

### Windows PowerShell

```powershell
.venv\Scripts\Activate.ps1
```

### Windows CMD

```cmd
.venv\Scripts\activate.bat
```

Con el entorno activado, instala las dependencias:

```bash
pip install -r requirements.txt
```

## Ejecucion

Desde la raiz del proyecto, ejecuta el flujo completo:

```powershell
python main.py
```

El programa extrae los datos de Whale Alert, genera el CSV en `data/` y lo
sube al bucket configurado en MinIO.

Para salir del entorno virtual:

```bash
deactivate
```

## Variables de entorno

El proyecto utiliza variables de entorno para configurar la conexion con MinIO.
Puedes definirlas en un archivo `.env` en la raiz del proyecto:

```env
MINIO_ENDPOINT=http://localhost:9000
MINIO_ACCESS_KEY=minioadmin
MINIO_SECRET_KEY=minioadmin
MINIO_BUCKET_NAME=whale-alert
```

El archivo `.env` se carga automaticamente desde Python mediante `python-dotenv` y
se leen las variables con `os.getenv`:

```python
import os
from dotenv import load_dotenv

load_dotenv()

endpoint = os.getenv("MINIO_ENDPOINT")
access_key = os.getenv("MINIO_ACCESS_KEY")
```

Tambien puedes crear variables de entorno directamente desde la terminal. Estas
variables seran temporales y se perderan al cerrar la terminal.

### Linux o macOS

```bash
export MINIO_ENDPOINT="http://localhost:9000"
export MINIO_ACCESS_KEY="minioadmin"
export MINIO_SECRET_KEY="minioadmin"
export MINIO_BUCKET_NAME="whale-alert"
python src/minio_client.py
```

### Windows PowerShell

```powershell
$env:MINIO_ENDPOINT = "http://localhost:9000"
$env:MINIO_ACCESS_KEY = "minioadmin"
$env:MINIO_SECRET_KEY = "minioadmin"
$env:MINIO_BUCKET_NAME = "whale-alert"
python src/minio_client.py
```

### Windows CMD

```cmd
set MINIO_ENDPOINT=http://localhost:9000
set MINIO_ACCESS_KEY=minioadmin
set MINIO_SECRET_KEY=minioadmin
set MINIO_BUCKET_NAME=whale-alert
python src/minio_client.py
```

Para comprobar una variable desde Python:

```python
import os

print(os.getenv("MINIO_ENDPOINT"))
```
