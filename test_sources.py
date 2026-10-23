from src.extract import fetch_source

print("FakeStoreAPI :")
print(fetch_source("fakestoreapi", "https://fakestoreapi.com", ["1", "2"])[1])

print("Platzi :")
print(fetch_source("platzi", "https://api.escuelajs.co/api/v1", ["9", "10"])[1])