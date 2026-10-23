CREATE TABLE dim_source (
  source_id  INT PRIMARY KEY,
  name       VARCHAR(50) NOT NULL UNIQUE,
  base_url   VARCHAR(255)
);

CREATE TABLE dim_product (
  product_id INT PRIMARY KEY,
  name       VARCHAR(150) NOT NULL,
  category   VARCHAR(80)
);

CREATE TABLE product_source_map (
  product_id  INT NOT NULL,
  source_id   INT NOT NULL,
  external_id VARCHAR(50) NOT NULL,
  PRIMARY KEY (product_id, source_id),
  FOREIGN KEY (product_id) REFERENCES dim_product(product_id),
  FOREIGN KEY (source_id)  REFERENCES dim_source(source_id)
);

-- Staging : vidée au début de chaque exécution
CREATE TABLE stg_price (
  product_id   INT,
  source_id    INT,
  collected_at DATETIME,
  list_price   DECIMAL(10,2),   
  discount_pct DECIMAL(5,2),    
  price        DECIMAL(10,2),
  currency     CHAR(3)
);

-- Table de faits : la clé d'unicité garantit l'idempotence
CREATE TABLE fact_price (
  product_id   INT NOT NULL,
  source_id    INT NOT NULL,
  collected_at DATETIME NOT NULL,
  list_price   DECIMAL(10,2) NOT NULL,   -- prix catalogue
  discount_pct DECIMAL(5,2)  NOT NULL DEFAULT 0,
  price        DECIMAL(10,2) NOT NULL,   -- prix final = ce qu'on analyse
  currency     CHAR(3) NOT NULL,
  ingested_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (product_id, source_id, collected_at),
  FOREIGN KEY (product_id) REFERENCES dim_product(product_id),
  FOREIGN KEY (source_id)  REFERENCES dim_source(source_id)
);

CREATE TABLE rejected_rows (
  id          BIGINT AUTO_INCREMENT PRIMARY KEY,
  source_id   INT,
  payload     JSON,
  reason      VARCHAR(255) NOT NULL,
  rejected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


CREATE TABLE pipeline_runs (
  run_id        BIGINT AUTO_INCREMENT PRIMARY KEY,
  collected_at  DATETIME NOT NULL,
  rows_read     INT DEFAULT 0,
  rows_rejected INT DEFAULT 0,
  rows_loaded   INT DEFAULT 0,
  status        VARCHAR(20),
  started_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  ended_at      TIMESTAMP NULL
);