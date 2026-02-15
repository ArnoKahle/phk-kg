# Integration: PHPP & Log-Daten

Ziel ist die Zusammenführung der statischen physikalischen Gebäudedaten (PHPP) mit den dynamischen Messwerten aus der Datenbank für den Knowledge Graph (KG).

## 1. Datenquellen
* **PHPP (Excel):** Liefert Konstanten, U-Werte, Flächen, Volumina und prognostizierte Energiebedarfe.
* **Log-Daten (Postgres):** Liefert reale Ist-Werte (Temperaturen, Zustände, Verbräuche).

## 2. Vorgehen für den Piloten
1.  **Identifikation Schlüsselwerte:**
    * Welche PHPP-Zellen (Named Ranges) entsprechen welchen Sensoren in der DB?
    * Mapping-Tabelle erstellen: `PHPP_Feld | DB_Sensor_ID | Einheit`.
2.  **Export aus PHPP:**
    * Relevante Blätter als CSV exportieren oder direkt via Skript (z.B. `pandas` / `openpyxl`) auslesen.
3.  **Anreicherung (Data Enrichment):**
    * Berechnung der "Gespeicherten Energie" (Thermische Masse):
    * `E_stored = Volumen * Dichte * Spez_Wärmekapazität * (T_Ist - T_Referenz)`
    * Die physikalischen Konstanten kommen aus dem PHPP, `T_Ist` aus den Logs.

## 3. Zielstruktur (KG)
* Verbindung der Entitäten via OEMetadata.
* Beispiel: `Sensor_A (DB)` -- *misst Temperatur von* --> `Raum_1 (PHPP-Definition)`.
