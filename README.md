* Praca dyplomowa *

Repozytorium zawiera projekt hurtowni danych zrealizowany w ramach pracy dyplomowej na kierunku Big Data. Data Engineering.

Informacje o pracy

*Tytuł pracy:*
„Budowa hurtowni danych w środowisku Microsoft SQL Server z wykorzystaniem architektury medalion na przykładzie sklepu e-commerce”

Autor: Joanna Szeterlak

-----------------------------------------------------------------------------------------------------------------------------------------------------------------------

🎯 **Cel projektu**

Celem projektu było zaprojektowanie i implementacja hurtowni danych w środowisku Microsoft SQL Server z wykorzystaniem architektury warstwowej typu Medallion (Bronze → Silver → Gold).

Projekt obejmuje:

→ implementację procesów ETL w T-SQL,

→ budowę modelu gwiazdy (star schema) w warstwie analitycznej,

→ zastosowanie mechanizmów kontroli jakości danych,

→ przygotowanie widoków analitycznych przeznaczonych do raportowania (np. Power BI).

-----------------------------------------------------------------------------------------------------------------------------------------------------------------------

🧱 **Architektura rozwiązania**

Projekt oparty jest na trójwarstwowej architekturze medalionu:

Bronze – warstwa danych surowych (ładowanie plików CSV)

Silver – warstwa transformacji i oczyszczania danych

Gold – warstwa analityczna (model gwiazdy + widoki)

Diagramy architektury znajdują się w katalogu Docs/.

-----------------------------------------------------------------------------------------------------------------------------------------------------------------------

🛠 **Technologie**

Microsoft SQL Server

T-SQL (procedury składowane, widoki, transformacje)

Architektura Medallion

Modelowanie wymiarowe (star schema)

(opcjonalnie) Microsoft Power BI

-----------------------------------------------------------------------------------------------------------------------------------------------------------------------

## 📂 Struktura repozytorium
```
Praca_dyplomoes/
│
├── Dane/                               # Pliki .csv z surowymi danymi syntetycznymi użytymi w projekcie
│
├── Docs/                               # Diagramy użyte w projekcie
│
└── Skrypty/                            # Wszystkie skrypty T-SQL użyte w implementacji hurtowni danych
    |
    └── Bronze                          # Kod DDL oraz procedura składowana
    └── Silver                          # Kod DDL oraz procedury składowane
        └── procedury czesciowe         # Procedury dla poszczególnych tabel w warstwie Silver
    └── Gold                            # Kod DDL oraz procedury składowane
        └── procedury czesciowe         # Procedury dla poszczególnych tabel w warstwie Gold
        └── widoki                      # Utworzone widoki analityczne
```

