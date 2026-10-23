import requests

URLS = {
    "dummyjson": "https://dummyjson.com/products?limit=40",
    "fakestoreapi": "https://fakestoreapi.com/products",
    "platzi": "https://api.escuelajs.co/api/v1/products?offset=0&limit=40",
}

with open("catalog.txt", "w", encoding="utf-8") as f:
    for name, url in URLS.items():
        data = requests.get(url, timeout=15).json()
        items = data["products"] if isinstance(data, dict) else data
        f.write(f"\n=== {name} ({len(items)} produits) ===\n")
        for it in items:
            f.write(f"{it['id']:>4} | {it['price']:>8} | {it['title'][:60]}\n")

print("Fichier catalog.txt créé")