import polars as pl
import argparse
import glob
import os
import time

def parse_arguments():
    parser = argparse.ArgumentParser(description="Robust FHEM Log Parser (V4 - Dirty Data Edition)")
    parser.add_argument("pattern", type=str, help="Dateipfad oder Pattern (z.B. 'data/*.log')")
    parser.add_argument("--limit", type=int, default=0, help="Nur X Zeilen lesen")
    parser.add_argument("--export", type=str, default=None, help="Export Pfad (z.B. 'data/processed')")
    return parser.parse_args()

def process_file_robust(file_path, limit=0):
    print(f"--> Verarbeite: {file_path}")
    
    # Check auf leere Datei (verhindert "Empty CSV" Fehler)
    if os.path.getsize(file_path) == 0:
        print("    WARNUNG: Datei ist leer (0 Bytes). Überspringe.")
        return None

    try:
        # STRATEGIE: Wir lesen ALLES als eine einzige Textspalte ("raw_line").
        # separator="\n" (Zeilenumbruch) statt " " (Leerzeichen).
        # quote_char=None ist wichtig, damit Anführungszeichen im Text nicht verwirren.
        q = pl.scan_csv(
            file_path,
            has_header=False,
            new_columns=["raw_line"],
            separator="\n", 
            quote_char=None,
            ignore_errors=True # Ignoriert Zeilen mit UTF-8 Fehlern (binärer Müll)
        )

        # Pipeline der Reinigung
        q = (
            q
            # 1. Filter: Null-Bytes (\000) entfernen (passiert bei Stromausfall)
            #    und Zeilen rauswerfen, die wir nicht wollen (CONNECTED)
            .filter(
                ~pl.col("raw_line").str.contains("\x00") & 
                ~pl.col("raw_line").str.contains("CONNECTED") &
                ~pl.col("raw_line").str.contains("Exiting") &
                ~pl.col("raw_line").str.contains("Server started")
            )
            
            # 2. Splitten: Wir erwarten "Timestamp Device Reading Value"
            # Wir nutzen split_exact. Das erzeugt ein Struct mit field_0 bis field_3.
            # Alles was danach kommt, wird ignoriert (löst das "too many fields" Problem!)
            .with_columns(
                pl.col("raw_line").str.split_exact(" ", 3) # Splitte max 3 mal -> 4 Teile
                .alias("parts")
            )
            
            # 3. Struct entpacken
            .unnest("parts")
            .rename({"field_0": "ts_raw", "field_1": "device", "field_2": "reading", "field_3": "value"})
            
            # 4. Filter: Prüfen ob wir wirklich 4 Teile hatten (sonst ist field_3 null)
            .filter(pl.col("value").is_not_null())

            # 5. Typisierung (Wie gehabt)
            .with_columns([
                pl.col("ts_raw").str.replace("_", " ")
                  .str.to_datetime("%Y-%m-%d %H:%M:%S", strict=False)
                  .alias("timestamp"),
                pl.col("reading").str.replace(":$", ""),
                # Versuchen Value in Zahl zu wandeln, Fehler -> null (aber Zeile bleibt)
                pl.col("value").cast(pl.Float64, strict=False).alias("value_num")
            ])
            
            # 6. Aufräumen
            .select(["timestamp", "device", "reading", "value", "value_num"])
            .filter(pl.col("timestamp").is_not_null())
        )

        if limit > 0:
            q = q.limit(limit)

        return q.collect()

    except Exception as e:
        print(f"    FEHLER bei {file_path}: {e}")
        return None

def main():
    args = parse_arguments()
    
    # Sortierte Dateiliste
    files = sorted(glob.glob(args.pattern))
    
    if not files:
        print(f"Keine Dateien gefunden für: {args.pattern}")
        return

    print(f"Gefundene Dateien: {len(files)}")
    
    total_rows = 0
    start_time = time.time()

    for f in files:
        df = process_file_robust(f, args.limit)
        
        if df is not None and not df.is_empty():
            rows = len(df)
            total_rows += rows
            # Info-Ausgabe
            try:
                min_ts = df["timestamp"].min()
                max_ts = df["timestamp"].max()
                print(f"    OK: {rows} Zeilen ({min_ts} bis {max_ts})")
            except:
                print(f"    OK: {rows} Zeilen (Kein gültiger Timestamp)")

            # Optional Export
            if args.export:
                os.makedirs(args.export, exist_ok=True)
                base_name = os.path.basename(f).replace(".log", ".parquet")
                out_path = os.path.join(args.export, base_name)
                df.write_parquet(out_path)
        else:
            print("    -> Keine verwertbaren Daten extrahiert.")

    duration = time.time() - start_time
    print("-" * 40)
    print(f"GESAMT: {total_rows} Datensätze.")
    print(f"Dauer:  {duration:.2f} Sek")
    if duration > 0:
        print(f"Speed:  {total_rows / duration:.0f} Zeilen/Sek")

if __name__ == "__main__":
    main()
