from datetime import datetime
from pathlib import Path

from src.minio_client import upload_file_to_minio
from src.whale_alert_extractor import save_to_csv, whale_alert_extractor


PROJECT_ROOT = Path(__file__).resolve().parent
DATA_DIR = PROJECT_ROOT / "data"


def main() -> int:
    DATA_DIR.mkdir(exist_ok=True)

    data = whale_alert_extractor()
    if data is None:
        print("[ERROR] No se pudieron extraer los datos.")
        return 1

    save_to_csv(data)

    file_path = DATA_DIR / f"whales_{datetime.now().strftime('%Y-%m-%d')}.csv"
    if not file_path.exists():
        print(f"[ERROR] No se genero el archivo CSV: {file_path}")
        return 1

    upload_file_to_minio(str(file_path))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
