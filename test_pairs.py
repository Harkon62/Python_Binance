from modul_db import connect_db_engine
import sys
import pandas as pd



def setPairsPrioritaet(dfpairs, dfpairs200):

    dfpairs.loc[:, 'Prioritaet'] = 9999


    # Dataframe durchlaufen um  Daten abzurufen
    for index, row in df_pairs200.iterrows():
        pair = row["pairs"]
        RecPos = index

        # Spalte close mit lowma vergleichen, Ergebnis = True = 1 setzen
        dfpairs.loc[
        (
            (dfpairs['pairs'] == pair)
        ),
        'Prioritaet'] = RecPos + 1

    dfpairs.sort_values('Prioritaet', inplace=True)


    return dfpairs


# Verbindung mit Datenbank aufbauen
engine = connect_db_engine()

# Abruf der cryptos aus DB
df_pairs = pd.read_sql('SELECT * FROM binance_pairs WHERE Prioritaet=1', engine)


# Abruf der cryptos aus DB GD200
df_pairs200 = pd.read_sql('SELECT pairs FROM `binance_gd200` ORDER BY `startDate` DESC', engine)



# pairs aus DB holen und nach Prioritaet Reihenfolge fuer den Downloads setzen
df_pairs = setPairsPrioritaet(df_pairs, df_pairs200)



print(df_pairs.head())

# Datenbankverbindung loesen
engine.dispose()

sys.exit()