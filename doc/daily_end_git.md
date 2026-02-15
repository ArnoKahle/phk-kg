Abschluss für heute: Git Push (Anleitung)

Da du gerade deine SSH-Keys eingerichtet hast, wollen wir sicherstellen, dass dein lokaler Stand sicher im Repository auf GitHub landet.

Öffne dein Terminal (dort wo dein Projekt liegt) und führe folgende Befehle nacheinander aus:
Schritt 1: Verbindung testen (Optional, aber gut zur Kontrolle)

Da du neue Keys hast, prüfe kurz, ob GitHub dich erkennt:
Bash

ssh -T git@github.com

Erwartete Antwort: Hi <username>! You've successfully authenticated...
Schritt 2: Status prüfen

Schau nach, welche Dateien geändert wurden:
Bash

git status

Rote Dateien: Geändert, aber noch nicht vorgemerkt.
Schritt 3: Änderungen vormerken (Staging)

Füge alle Änderungen hinzu (oder nenne spezifische Dateinamen statt .):
Bash

git add .

Schritt 4: Änderungen bestätigen (Commit)

Erstelle ein Paket mit einer Nachricht, was du heute getan hast:
Bash

git commit -m "WIP: SSH Keys eingerichtet, Import-Logik verbessert, DB-Schema Entwurf"

Schritt 5: Hochladen (Push)

Schiebe die Daten auf den Server:
Bash

git push

(Falls dies dein erster Push auf einen neuen Branch ist, sagt dir Git evtl., dass du git push --set-upstream origin <branchname> nutzen sollst. Einfach den Befehl kopieren, den Git dir vorschlägt).
