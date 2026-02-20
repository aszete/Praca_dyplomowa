# Praca dyplomowa

Repozytorium pracy dyplomowej na kierunku **Big Data. Data Engineering**

**Tytuł pracy:** „Budowa hurtowni danych w środowisku Microsoft SQL Server z wykorzystaniem architektury medalion na przykładzie sklepu e-commerce”

**Autor:** Joanna Szeterlak


## 📂 Struktura repozytorium
```
Praca_dyplomoes/
│
├── Dane/                               # Pliki .csv z surowymi danymi syntetycznymi użytymi w pracy
│
├── Docs/                               # Dodatkowe dokumenty, diagramy użyte w pracy
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

