import polars as pl
import os
import time

# Wir bleiben bei der Datei, um den Erfolg zu sehen
LOG_FILE = "data/raw/fhem/mbus-2024-03.log"

def process_fhem_log_v2():
    print(f"--- Starte Polars V2 (Smart Parse) mit {LOG_FILE} ---")
    
    if not os.path.exists(LOG_FILE):
        print(f"FEHLER: Datei nicht gefunden.")
        return

    start_time = time.time()

    # STRATEGIE:
    # Das FHEM System-Log ist chaotisch (unterschiedliche Spaltenanzahl).
    # Deshalb lesen wir es NICHT als CSV, sondern als reine Textzeilen
    # und extrahieren die Daten mit Regex. Das ist extrem robust.
    
    # Schema im Systemlog: "YYYY.MM.DD HH:MM:SS LEVEL MESSAGE"
    
    q = (
        pl.scan_csv(
            LOG_FILE, 
            has_header=False, 
            new_columns=["raw_line"], # Wir lesen alles in EINE Spalte
            separator="\n",           # Trenner ist Zeilenumbruch, nicht Leerzeichen!
            quote_char=None,
            ignore_errors=True
        )
        # Jetzt zerlegen wir die Zeile mit Regex (Regular Expressions)
        # Wir suchen nach: Datum (Capture 1) Leerzeichen Uhrzeit (Capture 2) Rest (Capture 3)
        .with_columns(
            pl.col("raw_line").str.extract_groups(r"^(\d{4}\.\d{2}\.\d{2})\s(\d{2}:\d{2}:\d{2})\s(\d+)\s(.*)$")
            .alias("parts")
        )
        # Wir werfen Zeilen weg, die nicht dem Muster entsprachen (null)
        .filter(pl.col("parts").is_not_null())
        
        # Jetzt die Struktur flachklopfen (Struct -> Columns)
        .unnest("parts")
        
        # Umbenennen der generierten Spalten (1, 2, 3, 4 aus der Regex-Gruppe)
        .rename({"1": "date", "2": "time", "3": "level", "4": "message"})
        
        # Datum und Zeit zu einem echten Timestamp verbinden
        .with_columns(
            (pl.col("date") + " " + pl.col("time"))
            .str.to_datetime("%Y.%m.%d %H:%M:%S") # Format anpassen (Punkte statt Striche!)
            .alias("timestamp")
        )
        
        # Nur relevante Spalten behalten
        .select(["timestamp", "level", "message"])
    )

    # Vorschau (jetzt mit collect und head, damit die Warnung weg ist)
    print("--- Saubere Vorschau ---")
    print(q.collect().head(5))

    print("\n--- Zähle valide Zeilen (nutzt jetzt hoffentlich mehr Cores) ---")
    # Zählen
    count = q.select(pl.len()).collect().item()
    
    end_time = time.time()
    duration = end_time - start_time
    
    print(f"Fertig! {count} saubere Datensätze extrahiert.")
    print(f"Zeit: {duration:.4f} Sek | Speed: {count / duration:.0f} Zeilen/Sek")

if __name__ == "__main__":
    process_fhem_log_v2()

