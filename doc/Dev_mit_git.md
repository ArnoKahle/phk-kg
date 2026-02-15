# Entwicklungsprozess & Versionierung

Dieser Prozess definiert den Workflow für die Code-Verwaltung und Sicherung des Projektstandes (Skripte, SQL-Definitionen, KG-Mappings).

## 1. Branching-Strategie
* **main:** Enthält ausschließlich stabilen, getesteten Code. Dies ist der Stand, der für den Piloten und die Produktivumgebung genutzt wird.
* **dev (development):** Der Hauptentwicklungszweig. Hier werden neue Features zusammengeführt.
* **feature/*:** (Optional) Für größere Module (z.B. `feature/thz-cleanup`), die vom `dev`-Branch abzweigen und nach Fertigstellung zurückgeführt werden.

## 2. Workflow-Schritte
1.  **Pull:** Vor Arbeitsbeginn den aktuellen Stand laden (`git pull`).
2.  **Commit:** Änderungen logisch gruppieren.
    * *Best Practice:* Klare Commit-Messages verwenden (z.B. "Fix: THZ Trigger Logik korrigiert" oder "Feat: Import-Skript für Device X hinzugefügt").
3.  **Push:** Änderungen regelmäßig in das Remote-Repository übertragen, um Datenverlust am Laptop vorzubeugen.

## 3. Tagging (Versionierung)
* Sobald ein Meilenstein erreicht ist (z.B. "Bereinigter Datenpool v1"), wird ein Tag im `main`-Branch gesetzt:
    * `git tag -a v0.1 -m "Basis-Importskripte stabil"`
