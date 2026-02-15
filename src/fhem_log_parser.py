import polars as pl
import argparse
import glob
import os
import time
from datetime import datetime

def parse_arguments():
    parser = argparse.ArgumentParser(description="High-Performance FHEM Log Parser (Polars based)")
    parser.add_argument("pattern", type=str, help="Dateipfad oder Pattern (z.B. 'data/raw/fhem/mbus-*.log')")
    parser.add_argument("--limit", type=int, default=0, help="Nur X Zeilen lesen (0 = alle)")
    parser.add_argument("--export", type=str, help="Pfad für Parquet-Export (optional)", default=None)
    return parser.parse_args()

def process_file(file_path, limit=0):
    print(f"--> Verarbeite: {file_path}")
    
    # Format-Erkennung anhand deiner "tail"-Ausgabe:
    # 2024-03-01_00:00:00 mbus 30-HC3-Lueftung-Waermeleistung: 100
    # Trenner ist Leerzeichen. 
    # Spalte 1: Timestamp (mit _)
    # Spalte 2: Device (mbus)
    # Spalte 3: Reading (mit :)
    # Spalte 4: Value
    
    try:
        q = (
            pl.scan_csv(
                file_path,
                separator=" ",
                has_header=False,
                new_columns=["ts_raw", "device", "reading_raw", "value"],
                # Wir ignorieren Zeilen mit falscher Spaltenzahl (manchmal sind Logs kaputt)
                ignore_errors=True 
            )
        )
        
        # Transformationen
        q = (
            q
            # 1. Timestamp bereinigen: "_" durch " " ersetzen und parsen
            .with_columns(
                pl.col("ts_raw").str.replace("_", " ")
                .str.to_datetime("%Y-%m-%d %H:%M:%S", strict=False) # strict=False macht null bei Fehlern
                .alias("timestamp")
            )
            # 2. Reading bereinigen: Doppelpunkt am Ende entfernen
            .with_columns(
                pl.col("reading_raw").str.replace(":$", "") # Regex: : am Ende
                .alias("reading")
            )
            # 3. Value bereinigen: Zahlen parsen (können Text oder Float sein)
            # Wir lassen es erstmal als String, um keine Daten zu verlieren, 
            # oder versuchen Float Cast
            .with_columns(
                pl.col("value").cast(pl.Float64, strict=False).alias("value_num")
            )
            # Selektieren
            .select(["timestamp", "device", "reading", "value", "value_num"])
            # Filtern: Nur Zeilen mit gültigem Timestamp
            .filter(pl.col("timestamp").is_not_null())
        )

        if limit > 0:
            q = q.limit(limit)

        # Ausführen!
        df = q.collect()
        return df

    except Exception as e:
        print(f"Fehler bei {file_path}: {e}")
        return None

def main():
    args = parse_arguments()
    
    # 1. Dateien finden (Globbing für Wildcards)
    # Da die Shell oft schon Wildcards auflöst, müssen wir prüfen:
    # Hat die Shell eine Liste übergeben oder einen String mit *?
    # Wir nehmen an, der User quoted den String: uv run ... "data/*.log"
    
    files = glob.glob(args.pattern)
    files.sort()
    
    if not files:
        print(f"Keine Dateien gefunden für Pattern: {args.pattern}")
        return

    print(f"Gefundene Dateien: {len(files)}")
    
    total_start = time.time()
    total_rows = 0
    all_dfs = []

    for f in files:
        df = process_file(f, args.limit)
        if df is not None:
            rows = len(df)
            print(f"    Gelesen: {rows} Zeilen")
            print(f"    Zeitraum: {df['timestamp'].min()} bis {df['timestamp'].max()}")
            total_rows += rows
            # Wir sammeln die DataFrames nicht im RAM, wenn wir nicht exportieren wollen,
            # sonst läuft der RAM voll bei 100 Dateien.
            # Hier nur Demo: Wir zeigen den Head der ersten Datei
            if total_rows == rows:
                print("--- Vorschau (Erste Datei) ---")
                print(df.head(5))

            if args.export:
                # Append Mode ist bei Parquet schwierig, besser pro Datei speichern
                # oder Partitioning nutzen. Hier einfachhalber:
                export_file = f"{args.export}_{os.path.basename(f)}.parquet"
                df.write_parquet(export_file)
                print(f"    Exportiert nach: {export_file}")

    total_duration = time.time() - total_start
    print("------------------------------------------------")
    print(f"GESAMT: {total_rows} Datensätze verarbeitet.")
    print(f"Laufzeit: {total_duration:.2f} Sekunden")
    print(f"Geschwindigkeit: {total_rows / total_duration:.0f} Zeilen/Sekunde")

if __name__ == "__main__":
    main()
