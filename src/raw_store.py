import json
from pathlib import Path




def raw_path (source_name,collected_at, base_dir="data/raw"):
    
    folder = Path(base_dir) / collected_at.strftime("%Y-%m-%d")
    return folder / f"{source_name}_{collected_at.strftime('%H%M')}.json"



    
def save_raw(source_name, collected_at, payloads, base_dir="data/raw"):

    path = raw_path(source_name, collected_at, base_dir)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payloads, ensure_ascii=False, indent=2), encoding="utf-8")
    return path

def load_raw(source_name, collected_at, base_dir="data/raw"):
    return json.loads(raw_path(source_name, collected_at, base_dir).read_text(encoding="utf-8"))