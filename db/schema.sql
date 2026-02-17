-- POSP payout schema for Excel-driven imports (MySQL)

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
  state_code VARCHAR(16) NULL,
  company VARCHAR(255),
  condition_text TEXT,
  final_payout DECIMAL(12,4),
  -- Numeric helper columns used by filtering logic
  cc_min INT NULL,
  cc_max INT NULL,
  gvw_min DECIMAL(12,4) NULL,
  gvw_max DECIMAL(12,4) NULL,
  watt_min INT NULL,
  watt_max INT NULL,
  age_min INT NULL,
  age_max INT NULL,
  applies_all_rto BOOLEAN DEFAULT FALSE,
  raw_json JSON,
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (import_id) REFERENCES imports(id),
  INDEX idx_rates_import (import_id),
  INDEX idx_rates_state_code (state_code)
);

CREATE TABLE IF NOT EXISTS rto (
  id INT AUTO_INCREMENT PRIMARY KEY,
  code VARCHAR(32) NOT NULL UNIQUE,
  name VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS rate_included_rto (
  rate_id BIGINT NOT NULL,
  rto_id INT NOT NULL,
  PRIMARY KEY (rate_id, rto_id),
  FOREIGN KEY (rate_id) REFERENCES rates(id) ON DELETE CASCADE,
  FOREIGN KEY (rto_id) REFERENCES rto(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS rate_excluded_rto (
  rate_id BIGINT NOT NULL,
  rto_id INT NOT NULL,
  PRIMARY KEY (rate_id, rto_id),
  FOREIGN KEY (rate_id) REFERENCES rates(id) ON DELETE CASCADE,
  FOREIGN KEY (rto_id) REFERENCES rto(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS query_log (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  ts DATETIME DEFAULT CURRENT_TIMESTAMP,
  state VARCHAR(64),
  rto VARCHAR(64),
  vehicle_type VARCHAR(128),
  fuel_type VARCHAR(128),
  policy_type VARCHAR(128),
  result_count INT
);
