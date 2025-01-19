import sqlite3
import pandas as pd
import numpy as np
import dash
from dash import dcc, html
import plotly.express as px
import warnings

# Ignorer les avertissements
warnings.filterwarnings("ignore", category=UserWarning)
warnings.filterwarnings("ignore", category=FutureWarning)

# --- ETAPE 1 : EXTRACTION DES DONNEES ---

# Fonction pour charger et nettoyer les données depuis les fichiers CSV
def load_and_clean_data(train_file, weather_file):
    # Charger les données depuis les fichiers CSV
    horaires_train_decembre = pd.read_csv(train_file)
    meteo_decembre = pd.read_csv(weather_file)

    # Supprimer les doublons
    horaires_train_decembre = horaires_train_decembre.drop_duplicates()
    meteo_decembre = meteo_decembre.drop_duplicates()

    # Supprimer les valeurs manquantes dans la colonne 'date'
    horaires_train_decembre = horaires_train_decembre.dropna(subset=['date'])
    meteo_decembre = meteo_decembre.dropna(subset=['date'])

    return horaires_train_decembre, meteo_decembre

# Fonction pour charger les données depuis SQLite
def read_from_sqlite(db_file, table_name):
    conn = sqlite3.connect(db_file)
    df = pd.read_sql(f"SELECT * FROM {table_name}", conn)
    conn.close()
    return df

# --- ETAPE 2 : TRANSFORMATION DES DONNEES ---

# Remplacer les valeurs nulles de l'heure de départ
def replace_null_departure_time(horaires_train_decembre):
    horaires_train_decembre['heure_depart_effectif'].fillna(horaires_train_decembre['heure_depart'], inplace=True)
    return horaires_train_decembre

# Fusionner les données de train et météo
def merge_data(horaires_train_decembre, meteo_decembre):
    jointure = pd.merge(horaires_train_decembre, meteo_decembre, on='date', how='inner')
    return jointure

# Calculer les retards
def calculate_delay(jointure):
    jointure['heure_depart'] = pd.to_datetime(jointure['heure_depart'])
    jointure['heure_depart_effectif'] = pd.to_datetime(jointure['heure_depart_effectif'])
    jointure['durée_du_retard'] = (jointure['heure_depart_effectif'] - jointure['heure_depart']).dt.total_seconds() / 60
    jointure['retardé'] = jointure['durée_du_retard'] >= 10
    return jointure

# Déterminer si la météo est mauvaise
def is_bad_weather(row):
    if row['temperature'] < 0 or row['precipitations'] > 10 or row['humidité'] > 80 or row['vent'] > 10:
        return True
    return False

# Ajouter les informations de météo et la cause du retard
def add_weather_and_delay_cause(jointure):
    jointure['mauvais_temps'] = jointure.apply(is_bad_weather, axis=1)
    jointure['cause_retard'] = np.where((jointure['retardé'] == True) & (jointure['mauvais_temps'] == True), 'Mauvais Temps', 'Autre Cause')
    return jointure

# Nettoyer les données et filtrer
def clean_and_filter_data(jointure):
    jointure.drop(columns=["temperature", "precipitations", "humidité", "vent", "retardé", 'mauvais_temps', "heure_depart_effectif", "heure_depart"], inplace=True)
    jointure = jointure[jointure['durée_du_retard'] >= 10]
    return jointure

# --- ETAPE 3 : CHARGEMENT DES DONNEES ---

# Fonction pour charger les données dans une base SQLite
def load_to_sqlite(df, table_name, db_file):
    conn = sqlite3.connect(db_file)
    df.to_sql(table_name, conn, if_exists='replace', index=False)
    conn.close()

# Initialisation de l'application Dash
app = dash.Dash(__name__)

# Charger et nettoyer les données
horaires_train_decembre, meteo_decembre = load_and_clean_data("projet DW2/trains_gare_de_lyon_december_2024.csv", "projet DW2/weather.csv")

# Remplacer les valeurs nulles de l'heure de départ
horaires_train_decembre = replace_null_departure_time(horaires_train_decembre)

# Fusionner les données de train et météo
jointure = merge_data(horaires_train_decembre, meteo_decembre)

# Calculer les retards
jointure = calculate_delay(jointure)

# Ajouter les informations de météo et la cause du retard
jointure = add_weather_and_delay_cause(jointure)

# Nettoyer les données et filtrer
jointure = clean_and_filter_data(jointure)

# Charger les données brutes et transformées dans la base SQLite
db_file = 'train_data.db'

# Charger les données brutes dans SQLite
load_to_sqlite(horaires_train_decembre, 'horaires_train', db_file)
load_to_sqlite(meteo_decembre, 'meteo', db_file)

# Charger les données transformées dans SQLite
load_to_sqlite(jointure, 'train_data', db_file)

# Relire les données depuis la base SQLite
horaires_train_from_db = read_from_sqlite(db_file, 'horaires_train')
meteo_from_db = read_from_sqlite(db_file, 'meteo')
train_data_from_db = read_from_sqlite(db_file, 'train_data')

# --- CREATION DES GRAPHIQUES ---

# Créer les graphiques à partir des données
fig_temperature = px.line(meteo_from_db, x='date', y='temperature', title="Températures de décembre", labels={'date': "Date", 'temperature': "Température (°C)"})
fig_delay = px.bar(train_data_from_db, x='cause_retard', y='durée_du_retard', title="Durée moyenne des retards par cause", labels={'cause_retard': "Cause du retard", 'durée_du_retard': "Durée moyenne (minutes)"})
fig_trains_per_day = px.bar(horaires_train_from_db.groupby('date').size().reset_index(name='nombre_de_trains'), x='date', y='nombre_de_trains', title="Nombre de trains par jour en décembre", labels={'date': "Date", 'nombre_de_trains': "Nombre de trains"})

# --- MISE EN PAGE DU TABLEAU DE BORD ---

# Définir la mise en page du tableau de bord
app.layout = html.Div([
    html.H1("Tableau de bord - Températures et Retards des Trains"),
    html.Div([
        dcc.Graph(
            id='trains-per-day-graph',
            figure=fig_trains_per_day
        ),
        dcc.Graph(
            id='temperature-graph',
            figure=fig_temperature
        ),
        dcc.Graph(
            id='delay-causes-graph',
            figure=fig_delay
        ),
    ], style={'display': 'flex', 'flex-direction': 'column', 'justify-content': 'space-between'})
])

# Lancer l'application Dash
if __name__ == '__main__':
    app.run_server(debug=True)
