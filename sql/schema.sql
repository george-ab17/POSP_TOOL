-- Minimal schema for imports and normalized rate tokens (MySQL)
CREATE TABLE IF NOT EXISTS imports (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  filename VARCHAR(255),
  uploaded_by VARCHAR(100),
  uploaded_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  effective_month DATE,
  status ENUM('pending','completed','failed') DEFAULT 'pending',
  row_count INT DEFAULT 0,
  notes TEXT
);

CREATE TABLE IF NOT EXISTS rates (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  import_id BIGINT NOT NULL,
  company VARCHAR(255),
  condition_text TEXT,
  final_payout DECIMAL(12,2),
  -- Numeric slab columns for different dimensions
  cc_min INT,
  cc_max INT,
  gvw_min INT,
  gvw_max INT,
  watt_min INT,
  watt_max INT,
  age_min INT,
  age_max INT,
  applies_all_rto BOOLEAN DEFAULT FALSE,
  raw_json JSON,
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (import_id) REFERENCES imports(id)
);

CREATE TABLE IF NOT EXISTS rto (
  id INT AUTO_INCREMENT PRIMARY KEY,
  code VARCHAR(64) UNIQUE,
  name VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS rate_included_rto (
  rate_id BIGINT,
  rto_id INT,
  PRIMARY KEY (rate_id, rto_id)
);

CREATE TABLE IF NOT EXISTS rate_excluded_rto (
  rate_id BIGINT,
  rto_id INT,
  PRIMARY KEY (rate_id, rto_id)
);
