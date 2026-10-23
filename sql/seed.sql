INSERT INTO dim_source (source_id, name, base_url) VALUES
  (1, 'dummyjson',    'https://dummyjson.com'),
  (2, 'fakestoreapi', 'https://fakestoreapi.com'),
  (3, 'platzi',       'https://api.escuelajs.co/api/v1')
ON DUPLICATE KEY UPDATE name = VALUES(name), base_url = VALUES(base_url);

INSERT INTO dim_product (product_id, name, category) VALUES
  (1, 'T-shirt',          'clothing'),
  (2, 'Eau de parfum',    'beauty'),
  (3, 'Sofa',             'furniture'),
  (4, 'Office chair',     'furniture')
ON DUPLICATE KEY UPDATE name = VALUES(name), category = VALUES(category);

INSERT INTO product_source_map (product_id, source_id, external_id) VALUES
  (1, 2, '2'),  (1, 3, '17'),
  (2, 1, '9'),  (2, 3, '47'),
  (3, 1, '12'), (3, 3, '28'),
  (4, 1, '14'), (4, 3, '34')
ON DUPLICATE KEY UPDATE external_id = VALUES(external_id);