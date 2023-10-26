#trouver creation StudyID ou faire code anonymisation et ajouter frequence de 5min
import os
from fastapi import FastAPI, Depends
from fastapi.responses import HTMLResponse
from sqlalchemy import create_engine, text
import pandas as pd
from dotenv import load_dotenv
import json

# Chargez les variables d'environnement du fichier .env
load_dotenv()

# Créez une instance de FastAPI
app = FastAPI()

USERNAME = os.getenv("SQL_USER")
PASSWORD = os.getenv("SQL_PASSWORD")
DATABASE_URL = f"postgresql://{USERNAME}:{PASSWORD}@spxp-app05:5432/cathydb"
engine = create_engine(DATABASE_URL)
conn = engine.connect()  # Créez une connexion

#EXCEL_PATH = "/home/melvinberto/VsCodeProjects/CHULA/backend/patients_CHLA.xlsx"

@app.get("/")
def execute_sql_query():
    #df = pd.read_excel(EXCEL_PATH, engine='openpyxl')
    adm_str=000000


    sqlcmd = f'''SELECT Patient, 
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
      MAX (CASE WHEN p.par like 'Measured Frequency'   THEN p.valnum END) AS resp_rate,
      MAX (CASE WHEN p.par like 'FC'   THEN p.valnum END) AS hr,
      MAX (CASE WHEN p.par like 'PEEP Setting' OR p.par like 'Positive End Expiratory Pressure (PEEP) Setting' THEN p.valnum END) AS peep,
      MAX (Case WHEN p.par like 'FiO2 mesuree' OR p.par like 'O2 Concentration Setting' or p.par like 'Inspired O2 (FiO2) Setting' THEN (p.valnum/100) END) AS fio2,
      MAX (CASE WHEN p.par like 'Mean airway pressure'  OR p.par like 'Mean Airway Pressure' THEN p.valnum END) AS map_value,
      MAX (CASE WHEN p.par like 'Tidal Volume Setting' THEN p.valnum END) AS setVte,
      MAX (CASE WHEN p.par like 'Expiratory Tidal Volume' THEN p.valnum END) AS vt,
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
      LEFT JOIN blood_gas b ON (b.noadmsip =p.noadmsip AND b.horodate BETWEEN (NOW()- interval '8 minutes') AND NOW())
      LEFT JOIN ptRespiratoryOrder r ON r.encounterId=p.noadmsip
      INNER JOIN D_Encounter l ON l.encounterid=p.noadmsip
     WHERE p.par IN ('SpO2','Measured Frequency','FC','PEEP Setting','PEEP réglée','PEEP reglee','O2 Concentration measured','O2 Concentration Setting','Mean Airway Pressure','Peak Airway Pressure'
      , 'Mean airway pressure','Tidal Volume Setting','Expiratory Tidal Volume','Inspiratory Time Setting','P0.1 Airway Pressure','CMV frequency Setting','CO2fe','Pression de crête','Inspired O2 (FiO2) Setting','Positive End Expiratory Pressure (PEEP) Setting','Measured Frequency')
      AND p.horodate BETWEEN (NOW()- interval '8 minutes') AND NOW()
      AND (l.lifetimenumber= ANY(array[{adm_str}]::text[]))
      GROUP BY l.encounterid, p.horodate, b.horodate
      ) x
      GROUP BY Patient, BloodGasTime;'''

    result = conn.execute(text(sqlcmd))
    final_df = pd.DataFrame(result.fetchall(), columns=result.keys())

    json_data = json.dumps(final_df.to_dict(orient="records"))
    return json_data