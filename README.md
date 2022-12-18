## Inhalt
* [Data Lake vs. Data Warehouse](#data-lake-vs-data-warehouse)
* [Cronjob vs. Airflow](#cron-vs-airflow)

## Data Lake vs. Data Warehouse
| Data Lake  | Data Warehouse |
| ---------- | -------------- |
| rohe Datenstruktur  | Daten sind verarbeitet  |
| Zweck/Schema der Daten noch nicht festgelegt  | Zweck/Schema der Daten in Formate, Strukturen überführt  |
| Gut zugänglich und schnell zu aktualisieren | Teuer und Kompliziert änderungen vorzunehmen |
| Anwender: Data Scientists | Anwender: Business Analysten |

Was ist ein Data Lake?
* Nimmt sämtliche Daten in Ursprungsformat auf (Rohformat, z.B. Audio, Video, etc.)
* Noch keine Verwendung festgelegt (Kommt drauf an aber in erster Instanz dem "Raw Layer" ja)
* Daten werden irgendwann in der Zukunft gebraucht
* Strukturierung oder Umformatierung erfolgt erst wenn die Daten benötigt werden, daher sehr schnell

Was ist ein Data Warehouse (DWH)?
* Repository für Strukturierte, gefilterte Daten 
* Sind bereits für bestimmten Zweck verarbeitet
* Struktur und Schema der Daten bereits definiert
* Für Analyse der Daten aus Transaktionssystemen optimiert

## Cron vs. Airflow
Cronjobs
* Können eine Aktion zu einem bestimmten Zeitpunkt ausführen

Airflow
* Können eine Aktion zu einem bestimmten Zeitpunkt ausführen
* Können "bedingt" ausgeführt werden, d.h. wenn eine Aktion durchgeführt wurde, wird automatisch die nächste dann durchgeführt
* Wichtige Operator sind PythonOperator, BashOperator & DateTimeOperator
