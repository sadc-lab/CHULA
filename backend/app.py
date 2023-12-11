
import os
from fastapi import FastAPI, Depends, Query, HTTPException
from sqlalchemy import create_engine, text
import pandas as pd
from dotenv import load_dotenv
import json
import logging
import httpx
from datetime import datetime
import pytz
import asyncio
from pytz import timezone
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from fastapi.responses import HTMLResponse


# Configuration du logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Chargement des variables d'environnement
load_dotenv()

# Configuration de la base de données
USERNAME = os.getenv("SQL_USER")
PASSWORD = os.getenv("SQL_PASSWORD")
DATABASE_URL = f"postgresql://{USERNAME}:{PASSWORD}@spxp-app05:5432/cathydb"
engine = create_engine(DATABASE_URL)
conn = engine.connect()  # Établissement de la connexion

# Création d'une instance FastAPI
app = FastAPI()

async def send_data(json_data):
    url = "https://i677xqk5rk.execute-api.us-west-2.amazonaws.com/Prod/api/monitorfeed"
    headers = {"Content-Type": "application/json", "X-Institution": "chsj"}
    auth = httpx.BasicAuth(username="jhotz", password="test")
    
    async with httpx.AsyncClient() as client:
        # prêt à envoyer les données réelles, décommentez la ligne ci-dessous
        # response = await client.post(url, json=json_data, headers=headers, auth=auth)
        # return response

        #cette ligne simule une réponse réussie sans envoyer de données
        return {"status": "success", "message": "Simulation d'envoi réussie"}

def enregistrer_historique(numero_dossier, study_id, date_heure):
    with open("historique_envois.txt", "a") as fichier:
        fichier.write(f"{numero_dossier} avec StudyID {study_id} a envoyé un JSON le {date_heure}\n")

@app.get("/")
async def execute_sql_query(adm_str: str = Query(..., title="adm_str", description="Patient's admission number"), study_id: str = None):
    logger.info("Exécution de la requête SQL pour adm_str: %s", adm_str)
  #3265868
#3445344
    # Requête SQL
    # sqlcmd2 = f'''SELECT 1;'''
    sqlcmd = text('''SELECT Patient, 
    min(minimumTime) minimumTime,
    percentile_cont(0.5) within group (order by spo2) spo2,
    percentile_cont(0.5) within group (order by resp_rate) resp_rate,
    percentile_cont(0.5) within group (order by hr) hr,
    percentile_cont(0.5) within group (order by peep) peep,
    percentile_cont(0.5) within group (order by fio2) fio2,
    percentile_cont(0.5) within group (order by map_value) map_value,
    percentile_cont(0.5) within group (order by setVte) setVte,
    percentile_cont(0.5) within group (order by vt) vt,
    percentile_cont(0.5) within group (order by inspiratory_time) inspiratory_time,
    percentile_cont(0.5) within group (order BY p01) p01,
    percentile_cont(0.5) within group (order by pip) pip,
    percentile_cont(0.5) within group (order by cast(ventrate AS FLOAT)) vent_rate,
    percentile_cont(0.5) within group (order by cast(etco2 AS FLOAT)) etco2,
    percentile_cont(0.5) within group (order by abg_ph) abg_ph,
    percentile_cont(0.5) within group (order by abg_pco2) abg_pco2,
    percentile_cont(0.5) within group (order by abg_hco3) abg_hco3,
    percentile_cont(0.5) within group (order by abg_pao2) abg_pao2,
    percentile_cont(0.5) within group (order by cbg_ph) cbg_ph,
    percentile_cont(0.5) within group (order by cbg_pco2) cbg_pco2,
    percentile_cont(0.5) within group (order by cbg_hco3) cbg_hco3,
    percentile_cont(0.5) within group (order by vbg_ph) vbg_ph,
    percentile_cont(0.5) within group (order by vbg_pco2) vbg_pco2,
    percentile_cont(0.5) within group (order by vbg_hco3) vbg_hco3,
    BloodGasTime
    FROM (
SELECT l.encounterid AS Patient,  
      MAX (CASE WHEN p.par like 'SpO2' THEN p.valnum END) AS spo2,
      MAX (CASE WHEN p.par like 'FR'   THEN p.valnum END) AS resp_rate,
      MAX (CASE WHEN p.par like 'FC'   THEN p.valnum END) AS hr,
      MAX (CASE WHEN p.par like 'PEEP Setting' OR p.par like 'Positive End Expiratory Pressure (PEEP) Setting' THEN p.valnum END) AS peep,
      MAX (Case WHEN p.par like 'FiO2 mesuree' OR p.par like 'O2 Concentration Setting' or p.par like 'Inspired O2 (FiO2) Setting' THEN (p.valnum/100) END) AS fio2,
      MAX (CASE WHEN p.par like 'Mean airway pressure'  OR p.par like 'Mean Airway Pressure' THEN p.valnum END) AS map_value,
      MAX (CASE WHEN p.par like 'Tidal Volume Setting' THEN p.valnum END) AS setVte,
      MAX (CASE WHEN p.par like 'Expiratory Tidal Volume' OR p.par like 'Expired Tidal Volume' THEN p.valnum END) AS vt,
      MAX (CASE WHEN p.par like 'Inspiratory Time Setting' THEN p.valnum END) AS inspiratory_time,
      MAX (CASE WHEN p.par like 'P0.1 Airway Pressure' THEN p.valnum END) AS p01,
      MAX (CASE WHEN p.par like 'Peak Airway Pressure' OR p.par like 'Pression de crête' THEN p.valnum END) AS pip,
      MAX (CASE WHEN p.par like 'CMV frequency Setting' or p.par like 'Measured Frequency' THEN p.valnum END) AS ventrate,
      MAX (CASE WHEN p.par like 'CO2fe' THEN p.valnum END) AS etco2,
      MAX(CASE WHEN p.par LIKE 'Pressure Control Level Above PEEP Setting' THEN p.valnum END) AS Ps,
      p.horodate AS minimumTime,
      MAX (CASE WHEN b.site LIKE 'ARTERIEL' THEN b.ph END) AS abg_ph,
      MAX (CASE WHEN b.site LIKE 'ARTERIEL' THEN b.pco2 END) AS abg_pco2,
      MAX (CASE WHEN b.site LIKE 'ARTERIEL' THEN b.hco3 END) AS abg_hco3,
      MAX (CASE WHEN b.site LIKE 'ARTERIEL' THEN b.po2 END) AS abg_pao2,
      MAX (CASE WHEN b.site LIKE 'CAPILLAIRE' THEN b.ph END) AS cbg_ph,
      MAX (CASE WHEN b.site LIKE 'CAPILLAIRE' THEN b.pco2 END) AS cbg_pco2,
      MAX (CASE WHEN b.site LIKE 'CAPILLAIRE' THEN b.hco3 END) AS cbg_hco3,
      MAX (CASE WHEN b.site LIKE 'VEINEUX' THEN b.ph END) AS vbg_ph,
      MAX (CASE WHEN b.site LIKE 'VEINEUX' THEN b.pco2 END) AS vbg_pco2,
      MAX (CASE WHEN b.site LIKE 'VEINEUX' THEN b.hco3 END) AS vbg_hco3,
      b.horodate AS BloodGasTime  		  
      FROM icca_htr p
      LEFT JOIN blood_gas b ON (b.noadmsip =p.noadmsip AND b.horodate BETWEEN (NOW()- interval '3600 minutes') AND NOW())
      LEFT JOIN ptRespiratoryOrder r ON r.encounterId=p.noadmsip
      INNER JOIN D_Encounter l ON l.encounterid=p.noadmsip
     WHERE p.par IN ('SpO2','FR','FC','PEEP Setting','PEEP réglée','PEEP reglee','O2 Concentration measured','O2 Concentration Setting','Mean Airway Pressure','Peak Airway Pressure'
      , 'Mean airway pressure','Tidal Volume Setting','Expiratory Tidal Volume','Inspiratory Time Setting','P0.1 Airway Pressure','CMV frequency Setting','CO2fe','Pression de crête','Inspired O2 (FiO2) Setting','Positive End Expiratory Pressure (PEEP) Setting','Measured Frequency')
      AND p.horodate BETWEEN (NOW()- interval '5 minutes') AND NOW()
      AND l.lifetimenumber = :adm_str
      GROUP BY l.encounterid, p.horodate, b.horodate
      ) x
      GROUP BY Patient, BloodGasTime;''')

    
    # Exécution de la requête SQL
    result = conn.execute(sqlcmd, {'adm_str': adm_str})
    logger.info("Requête SQL exécutée avec succès")

    # Conversion des résultats en DataFrame
    final_df = pd.DataFrame(result.fetchall(), columns=result.keys())
    logger.info("Résultats convertis en DataFrame")

    if 'resp_rate' in final_df.columns and 'vt' in final_df.columns:
            # Convertir vt en litres si nécessaire (par exemple, si vt est en mL)
            final_df['vt'] = final_df['vt'] / 1000 

            # Calculer mve (en L/min)
            final_df['mve'] = final_df['resp_rate'] * final_df['vt']
    
    # Renommage des colonnes pour correspondre au format JSON LA
    renamed_columns = {
        'patient': 'studyID',
        'minimumtime': 'minimumTime',
        'resp_rate': 'rr',
        'bloodgastime': 'BloodGasTime',
        # Ajoutez ici d'autres renommages si nécessaire
    }
    final_df.rename(columns=renamed_columns, inplace=True)

    # Suppression des colonnes qui ne sont pas dans JSON LA
    columns_to_keep = ['studyID', 'minimumTime', 'etco2', 'fio2', 'hr', 'map_value', 'mve', 'peep', 'pip', 'rr', 'spo2', 'vt', 'BloodGasTime', 'vbg_be', 'vbg_o2sat', 'vbg_pco2', 'vbg_ph', 'vbg_po2']
    final_df = final_df[[col for col in columns_to_keep if col in final_df.columns]]

    # Ajout des colonnes manquantes avec des valeurs par défaut
    for column in columns_to_keep:
        if column not in final_df:
            final_df[column] = None

    # Réordonner les colonnes pour correspondre à l'ordre dans JSON LA
    final_df = final_df[columns_to_keep]

    # Vérification des données avant la conversion en JSON
    print(final_df)
    
    if study_id is not None:
        final_df['studyID'] = study_id

    # Conversion en JSON avec un formatage lisible

    json_data = json.dumps(json.loads(final_df.to_json(orient="records", date_format='iso')), indent=4)
    logger.info("Données converties en JSON")
    print(json_data)
    return json_data


# Liste pour suivre les dossiers en cours
dossiers_en_cours = []


# Fonction pour traiter les données après la récupération de la base de données
async def process_data(adm_str, study_id):
    json_data = await execute_sql_query(adm_str=str(adm_str), study_id=study_id)

    # Convertir les données JSON en DataFrame pour un traitement plus facile
    data_df = pd.read_json(json_data)

    # Trier le DataFrame par studyID et par BloodGasTime en ordre décroissant
    data_df.sort_values(by=['studyID', 'BloodGasTime'], ascending=[True, False], inplace=True)

    # Conserver uniquement la première occurrence de chaque studyID
    return data_df.drop_duplicates(subset='studyID', keep='first')


@app.get("/process-csv")
async def process_csv(file_path: str = "./bdd.csv"):
    global dossiers_en_cours

    try:
        df = pd.read_csv(file_path)
        utc = timezone('UTC')
        montreal = timezone('America/Montreal')
        for _, row in df.iterrows():
            numero_dossier = row['numero_dossier']
            study_id = row['studyID']
            date_debut_utc = utc.localize(pd.to_datetime(row['date_debut']))
            date_fin_utc = utc.localize(pd.to_datetime(row['date_fin']))
            date_debut_montreal = date_debut_utc.astimezone(montreal)
            date_fin_montreal = date_fin_utc.astimezone(montreal)
            now = datetime.now(montreal)

            if date_debut_montreal <= now <= date_fin_montreal:
                combined_df = pd.DataFrame()  # Créer un nouveau DataFrame pour chaque patient
                processed_data = await process_data(numero_dossier, study_id)
                combined_df = pd.concat([combined_df, processed_data])
                logger.info(f"Numéro de dossier {numero_dossier} traité")

                if not combined_df.empty:
                    combined_json = combined_df.to_json(orient='records')
                    response = await send_data(combined_json)
                    logger.info(f"Envoi de données combinées, réponse : {response['message']}")

                    # Enregistrement dans le fichier historique
                    now_str = now.strftime('%Y-%m-%d %H:%M:%S')
                    enregistrer_historique(numero_dossier, study_id, now_str)

                    # Mise à jour de dossiers_en_cours
                    dossiers_en_cours.append({
                        "numero_dossier": numero_dossier, 
                        "studyID": study_id, 
                        "dernier_envoi": now_str,
                        "json_envoye": combined_json
                    })
                else:
                    logger.info("Aucune donnée à envoyer après traitement")

        return {"message": "Traitement du fichier CSV terminé"}
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="Fichier CSV non trouvé")
    except Exception as e:
        logger.error(f"Erreur dans process_csv: {e}")

scheduler = AsyncIOScheduler()
scheduler.add_job(process_csv, 'interval', minutes=5)
scheduler.start()


@app.get("/dossiers-en-cours", response_class=HTMLResponse)
async def afficher_dossiers_en_cours():
    html_content = "<html><body><h1>Dossiers en Cours d'Envoi</h1><ul>"
    derniers_envois = {}

    # Création d'un dictionnaire avec le dernier envoi pour chaque studyID
    for dossier in dossiers_en_cours:
        study_id = dossier['studyID']
        if study_id not in derniers_envois or dossier['dernier_envoi'] > derniers_envois[study_id]['dernier_envoi']:
            derniers_envois[study_id] = dossier

    # Affichage des derniers envois
    for study_id, dossier in derniers_envois.items():
        json_pretty = json.dumps(json.loads(dossier['json_envoye']), indent=4)
        html_content += f"<li>{dossier['numero_dossier']} (StudyID: {study_id}) - Dernier envoi: {dossier['dernier_envoi']}<br><pre>{json_pretty}</pre></li><br>"

    html_content += "</ul></body></html>"
    return html_content
