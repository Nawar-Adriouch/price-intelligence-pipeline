import logging
from datetime import datetime
from dotenv import load_dotenv

from src.extract import fetch_source
from src.raw_store import save_raw
from src.load import get_conn, get_source, get_external_ids, load_staging, upsert_final

load_dotenv()
logging.basicConfig(level=logging.INFO)

# Créneau logique fixé à la main pour le test (Airflow le fournira plus tard)
COLLECTED_AT = datetime(2026, 10, 19, 8, 0)

conn = get_conn()

# IDs de tes 3 sources dans la table dim_source (1: dummyjson, 2: fakestoreapi, 3: platzi)
sources_to_run = [1, 2, 3]

for source_id in sources_to_run:
    name, base_url = get_source(conn, source_id)
    external_ids = get_external_ids(conn, source_id)
    
    if not external_ids:
        logging.warning(f"Aucun ID externe trouvé pour la source {name} (ID: {source_id})")
        continue

    logging.info(f"--- Traitement de la source : {name} (ID: {source_id}) ---")

    try:
        # Appel de la fonction générique d'extraction avec le nom de la source
        raw, rows = fetch_source(name, base_url, external_ids)

        # Sauvegarde RAW, Staging et Upsert
        path = save_raw(name, COLLECTED_AT, raw)
        n = load_staging(conn, rows, source_id, COLLECTED_AT)
        upsert_final(conn, source_id, COLLECTED_AT)

        print(f"[{name}] -> raw: {path} | staging: {n} lignes | upsert terminé")

    except Exception as e:
        logging.error(f"Erreur lors du traitement de la source {name}: {e}")

conn.close()
print("--- Traitement de toutes les sources terminé ---")