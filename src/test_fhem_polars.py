import polars as pl
import os
import time

# Pfad anpassen, falls deine Datei anders heißt
LOG_FILE = "data/raw/fhem/sample.log"

def process_fhem_log():
    print(f"--- Starte Polars Test mit {LOG_FILE} ---")
    
    if not os.path.exists(LOG_FILE):
        print(f"FEHLER: Datei {LOG_FILE} nicht gefunden. Bitte kopiere ein Logfile dorthin.")
        return

    start_time = time.time()

    # FHEM Logs sind meistens so aufgebaut:
    # 2024-01-01_00:00:01 DeviceName Reading: Value
    # Trennzeichen ist ein Leerzeichen.
    
    # Wir nutzen LazyFrame (scan_csv) für Performance
    # has_header=False, da Logs keine Überschrift haben
    q = (
        pl.scan_csv(
            LOG_FILE, 
            separator=" ", 
            has_header=False, 
            ignore_errors=True, # Falls mal eine Zeile kaputt ist
            new_columns=["timestamp", "device", "reading_raw", "value", "extra"] # Wir raten mal 5 Spalten
        )
        # Filterung: Wir wollen nur Zeilen, die wirklich wie ein Reading aussehen
        # Beispiel: Reading endet oft mit Doppelpunkt "temperature:"
        .filter(pl.col("reading_raw").str.contains(":"))
        
        # Bereinigung: Doppelpunkt aus dem Reading-Namen entfernen
        .with_columns(
            pl.col("reading_raw").str.replace(":", "").alias("reading")
        )
        
        # Selektieren der relevanten Spalten
        .select(["timestamp", "device", "reading", "value"])
    )

    # Jetzt feuern wir die Query ab (Action!)
    # fetch(5) holt nur die ersten 5 zum gucken
    print("--- Vorschau (Top 5) ---")
    print(q.fetch(5))

    # Jetzt zählen wir mal alle Zeilen (das zwingt Polars, die ganze Datei zu lesen)
    print("\n--- Zähle alle Zeilen (Performance Test) ---")
    count = q.select(pl.len()).collect().item()
    
    end_time = time.time()
    duration = end_time - start_time
    
    print(f"Fertig! {count} Datensätze verarbeitet.")
    print(f"Benötigte Zeit: {duration:.4f} Sekunden")
    print(f"Geschwindigkeit: {count / duration:.0f} Zeilen/Sekunde")

if __name__ == "__main__":
    process_fhem_log()
