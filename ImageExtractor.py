import os
import ast
import cv2
import numpy as np
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta
from pymongo import MongoClient
import gridfs

def process_image_csv(csv_path: Path, suffix: str):
    """
    Dla danego pliku CSV:
    - zapisuje obrazy lokalnie w folderze extracted_images_{suffix}
    - wrzuca raw bytes do GridFS w DB ros_images_{suffix}
    - wrzuca wpisy time-series do kolekcji image_timeseries w tej samej bazie
    """
    print(f"\n>> Przetwarzam zestaw '{suffix}' z pliku {csv_path.name}")
    df = pd.read_csv(csv_path)
    if 'data' not in df.columns:
        raise RuntimeError(f"[{suffix}] Brak kolumny 'data' w {csv_path}")

    # katalog na pliki
    output_dir = Path(rf'C:\Users\rimpe\Desktop\extracted_images_{suffix}')
    output_dir.mkdir(parents=True, exist_ok=True)

    # połączenie z MongoDB i GridFS
    client = MongoClient("mongodb://localhost:27017/")
    db_name = f"ros_images_{suffix}"
    db = client[db_name]
    fs = gridfs.GridFS(db)

    # utworzenie kolekcji time-series
    ts_name = "image_timeseries"
    if ts_name not in db.list_collection_names():
        db.create_collection(
            ts_name,
            timeseries={
                "timeField": "timestamp",
                "metaField": "metadata",
                "granularity": "seconds"
            }
        )
    ts_coll = db[ts_name]

    for idx, row in df.iterrows():
        # parsowanie raw bytes
        try:
            raw = ast.literal_eval(row['data'])
            if not isinstance(raw, (bytes, bytearray)):
                raw = bytes(raw)
        except Exception as e:
            print(f"[{suffix}][{idx}] Błąd parsowania danych: {e}")
            continue

        # dekodowanie obrazu
        arr = np.frombuffer(raw, dtype=np.uint8)
        img = cv2.imdecode(arr, cv2.IMREAD_COLOR)
        if img is None:
            print(f"[{suffix}][{idx}] cv2.imdecode zwróciło None")
            continue

        # zapis lokalny
        fname = f"{suffix}_frame_{idx:05d}.png"
        local_path = output_dir / fname
        cv2.imwrite(str(local_path), img)

        # zapis do GridFS
        try:
            file_id = fs.put(
                raw,
                filename=fname,
                metadata={"index": idx, "set": suffix}
            )
        except Exception as e:
            print(f"[{suffix}][{idx}] Błąd GridFS: {e}")
            continue

        # budowa timestampa z kolumn CSV
        try:
            sec  = int(row['header.stamp.secs'])
            nsec = int(row['header.stamp.nsecs'])
            timestamp = datetime.utcfromtimestamp(sec) + timedelta(microseconds=nsec // 1000)
        except Exception as e:
            print(f"[{suffix}][{idx}] Błąd parsowania timestampa: {e}")
            continue

        # wpis do kolekcji time-series
        ts_coll.insert_one({
            "timestamp": timestamp,
            "metadata": {
                "file_id":    file_id,
                "filename":   fname,
                "local_path": str(local_path),
                "index":      idx
            }
        })

        if idx % 50 == 0:
            print(f"[{suffix}] przetworzono {idx+1}/{len(df)} klatek")

    print(f"[{suffix}] Gotowe – obrazy w {output_dir}, GridFS w DB '{db_name}', TS-coll '{ts_name}'.")


if __name__ == "__main__":
    # Dokładne ścieżki do Twoich dwóch CSV:
    sets = [
        (
            Path(r'C:\Users\rimpe\Desktop\Projekty_Python\HurtownieDanychProjekt\venv\Data\D_trajectories\pointgrey-camera-image_color-compressed.csv'),
            'D'
        ),
        (
            Path(r'C:\Users\rimpe\Desktop\Projekty_Python\HurtownieDanychProjekt\venv\Data\H_trajectories\pointgrey-camera-image_color-compressed.csv'),
            'H'
        ),
    ]

    for csv_path, suffix in sets:
        process_image_csv(csv_path, suffix)