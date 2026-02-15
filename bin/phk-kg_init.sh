# Hauptverzeichnis erstellen (Namen anpassen)
#mkdir -p passivehouse-kg
cd /home/phk-kg

# Unterverzeichnisse
mkdir -p data/raw/phpp       # Hier kommt dein Excel rein
mkdir -p data/raw/fhem       # Hier kommen die Logs rein
mkdir -p data/processed      # Hier landen unsere CSV/JSONs
mkdir -p data/ontology       # Für Turtle (.ttl) Dateien
mkdir -p notebooks           # Für Jupyter Notebooks
mkdir -p src                 # Für Python Skripte
mkdir -p docs                # Dein Lab-Diary

# README und Lab-Diary anlegen
echo "# Passive House Knowledge Graph Project" > README.md
echo "# Lab Diary - Scientific Log" > docs/lab_diary.md
