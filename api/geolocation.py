# ---
# jupyter:
#   jupytext:
#     cell_metadata_filter: -all
#     formats: py:percent,ipynb
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.18.1
# ---

# %%
# Kirjastojen tuonti
import urllib.parse
import json
import pandas as pd
import re # Tarvitaan regular expressioneille
from pathlib import Path 
from datetime import datetime, timedelta 

from dash import dcc, html, Dash
from dash.dependencies import Output, Input
import plotly.express as px


# %%
# 23 kuntaa (Tarvitaan kuntalistan luomiseen/tunnistukseen)
canonical_cities = [
    "Iisalmi", "Keitele", "Kiuruvesi", "Kuopio", "Lapinlahti", 
    "Leppävirta", "Pielavesi", "Siilinjärvi", "Sonkajärvi", "Suonenjoki", 
    "Tervo", "Varkaus", "Vesanto", "Rautavaara", "Vieremä", 
    "Joensuu", "Joroinen", "Pieksämäki", 
    "Hankasalmi", "Konnevesi", "Rautalampi", 
    "Laukaa", "Äänekoski"
]

# %%
# Vanhojen kuntien ja nykyisten kuntien nimenmuutokset
location_map = {
    'Nilsiä': 'Kuopio', 
    'Tahkovuori': 'Kuopio', 
    'Juankoski': 'Kuopio', 
    'Varpaisjärvi': 'Lapinlahti'
}

# %%
# --- UUSI: Avainsanojen normalisointifunktio (CAUSE ANALYSIS) ---
def normalize_cause(tag):
    if not isinstance(tag, str):
        return None

    # Varmista, että käsittelet Unicode-escape koodit (esim. \u00e4)
    tag_decoded = tag.encode('utf-8').decode('unicode-escape').lower().strip()

    # --- Maintenance (huolto*) ---
    if re.search(r"\bhuol", tag_decoded):
        return "Huolto"

    # --- Digging (kaivuutyöt*) ---
    if re.search(r"\bkaiv", tag_decoded):
        return "Kaivuutyöt"

    # --- Renovation/Saneer (saneer*) ---
    if re.search(r"\bsaneer", tag_decoded):
        return "Saneeraus"
    
    # --- Repair (korj*) ---
    if re.search(r"\bkorj", tag_decoded):
        return "Korjaustyöt"
    
    # --- Damage/Fault repair (vaurio*) ---
    if re.search(r"\bvaurio", tag_decoded):
        return "Vauriokorjaus"

    # --- If no pattern matches → DROP the tag ---
    return None



# %%
# --- Stream Datan Lataus (Simuloitu) ---
FILE_PATH = Path(r'E:\projects\python\data-pipeline\data\processed\outage_data.json') 

try:
    with open(FILE_PATH, 'r', encoding='utf-8') as f:
        STREAM_DATA = json.load(f)
except FileNotFoundError:
    print(f"FATAL ERROR: outage_data.json not found at {FILE_PATH}.")
    raise FileNotFoundError(f"outage_data.json ei löydy polusta: {FILE_PATH}")

# %%
# Globaali tila Dash-sovelluksen käyttöön
stream_state = {
    'index': 0,
    # Tagit ovat nyt normalisoituja syy-kategorioita
    'tag_counts': {}, 
    # Kuntien lukumäärä
    'location_counts': {}
}
print(f"Stream size: {len(STREAM_DATA)} events.")

# %%
# Initialize Dash app
app = Dash(__name__)

# %%
# --- Dashboard Asettelu (Layout) ---
app.layout = html.Div([
    html.H1("Savon Voima Oy Häiriöanalyysi", style={'textAlign': 'center'}),
    
    # Ylätason sisältöalue (Donitsi ja Palkkikaavio rinnakkain)
    html.Div([
        # 1. Donitsikaavio 
        html.Div(dcc.Graph(id='live-donut-chart'), 
                 style={'width': '50%', 'display': 'inline-block', 'padding': '10px'}),
        
        # 2. Palkkikaavio (Bar Chart)
        html.Div(dcc.Graph(id='live-bar-chart'), 
                 style={'width': '50%', 'display': 'inline-block', 'padding': '10px'}),
    ], style={'display': 'flex'}),
    
    # 3. Teksti-ilmoitus (Latest Event)
    html.Div([
        html.H3("Viimeisin tapahtuma:", style={'margin-top': '20px'}),
        html.Div(id='latest-event-text', style={'fontSize': '18px', 'padding': '10px', 'backgroundColor': '#f5f5f5'}),
    ]),
    
    # 4. Aikasarjakaavio (Cumulative Events)
    html.Div(dcc.Graph(id='cumulative-events-chart')),
    
    # Interval Component: Syke, joka käynnistää päivityksen 250ms välein
    dcc.Interval(
        id='interval-component',
        interval=250, # 250 milliseconds (0.25s)
        n_intervals=0
    )
])

# %%
# --- DASH CALLBACK: Päivitysfunktio ---
@app.callback(
    [Output('live-donut-chart', 'figure'), 
     Output('live-bar-chart', 'figure'),
     Output('latest-event-text', 'children'),
     Output('cumulative-events-chart', 'figure')],
    Input('interval-component', 'n_intervals')
)
def update_dashboard(n):
    global stream_state
    
    i = stream_state['index']
    
    # Jos stream loppuu, aloitetaan alusta
    if i >= len(STREAM_DATA):
        stream_state['index'] = 0 
        i = 0
        stream_state['tag_counts'] = {} 
        stream_state['location_counts'] = {}
    
    new_entry_raw = STREAM_DATA[i]
    
    location_name_raw = new_entry_raw.get('location')
    location_name = location_map.get(location_name_raw, location_name_raw)

    # Laske kesto tunneissa (logiikka pysyy samana)
    duration_h = 0
    try:
        start_str = new_entry_raw['time_start'].replace('.', ':')
        end_str = new_entry_raw['time_end'].replace('.', ':')
        time_format = '%H:%M' if ':' in start_str else '%H'
        
        t_start = datetime.strptime(start_str, time_format)
        t_end = datetime.strptime(end_str, time_format)
        
        time_delta = t_end - t_start
        if time_delta.total_seconds() < 0:
             time_delta = timedelta(days=1) + time_delta
             
        duration_h = time_delta.total_seconds() / 3600

    except ValueError:
        duration_h = 0
        
    # 3. Päivitä globaali Tila
    
    # A) Avainsanojen laskenta - KÄYTÄÄN normalize_cause-funktiota
    current_normalized_tags = []
    for tag in new_entry_raw.get('tags', []):
        normalized_cause = normalize_cause(tag)
        if normalized_cause:
            # LISÄTTY: Laske vain normalisoidut ja hyväksytyt syyt
            stream_state['tag_counts'][normalized_cause] = stream_state['tag_counts'].get(normalized_cause, 0) + 1
            current_normalized_tags.append(normalized_cause) # Kerätään teksti-ilmoitusta varten

    # B) Sijaintien laskenta
    stream_state['location_counts'][location_name] = stream_state['location_counts'].get(location_name, 0) + 1


    stream_state['index'] += 1

    # --- ELEMENT 1: Donitsikaavio (Avainsanojen osuus) ---
    tag_df = pd.DataFrame(
        list(stream_state['tag_counts'].items()), 
        columns=['Kategoria', 'Lukumäärä']
    )
    
    if tag_df.empty:
        donut_fig = px.pie(title=f"Keskeytysten syyt (Tapahtumat yhteensä: {i+1})")
    else:
        donut_fig = px.pie(
            tag_df, 
            values='Lukumäärä', 
            names='Kategoria', 
            title=f"Keskeytysten syyt (Tapahtumat yhteensä: {i+1})",
        )
        donut_fig.update_traces(hole=.4, textinfo='label+percent') 
        
    # --- ELEMENT 2: Bar Chart (Top 5 Kunnat) ---
    bar_df = pd.DataFrame(
        list(stream_state['location_counts'].items()), 
        columns=['Kunta', 'Lukumäärä']
    )
    bar_df = bar_df.sort_values('Lukumäärä', ascending=False).head(5)

    bar_fig = px.bar(
        bar_df, 
        x='Kunta', 
        y='Lukumäärä', 
        title="5 eniten mainittua kuntaa",
        labels={'Lukumäärä': 'Tapahtumien lkm'}
    )

    # --- ELEMENT 3: Text Display ---
    # Nyt näytetään vain normalisoidut syyt, jos niitä löytyy
    tag_display = ', '.join(current_normalized_tags) if current_normalized_tags else "Ei tunnistettua syytä"
    
    latest_event_text = (
        f"Sijainti: **{location_name_raw}** | Kesto: {new_entry_raw['time_start']} - {new_entry_raw['time_end']} (**{duration_h:.2f} h**)"
        f" | Syy-kategoria: {tag_display}"
    )
    
    # --- ELEMENT 4: Cumulative Events Chart ---
    cumulative_events_fig = px.line(
        x=list(range(i+1)), 
        y=[n for n in range(i+1)], 
        title="Kumulatiivinen tapahtumamäärä",
        labels={'x': 'Kulunut sykeaika (x 250ms)', 'y': 'Tapahtumien kokonaislkm'}
    )

    return donut_fig, bar_fig, latest_event_text, cumulative_events_fig


# %%
# Aja sovellus VS Code Jupyter Notebookissa
app.run(port=8050)

# %%