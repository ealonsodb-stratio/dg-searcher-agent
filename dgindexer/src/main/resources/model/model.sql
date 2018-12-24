SELECT EXISTS(SELECT 1 from pg_database WHERE datname='dg_database');
CREATE DATABASE dg_database;
\c dg_database;

SELECT EXISTS(SELECT 1 FROM information_schema.schemata WHERE schema_name = 'dg_metadata');
CREATE SCHEMA dg_metadata;
CREATE TABLE IF NOT EXISTS dg_metadata.data_asset(
    id SERIAL NOT NULL UNIQUE PRIMARY KEY,
    name TEXT,
    alias TEXT,
    description TEXT,
    metadata_path TEXT NOT NULL UNIQUE,
    type TEXT NOT NULL,
    subtype TEXT NOT NULL,
    tenant TEXT NOT NULL,
    properties JSONB NOT NULL,
    active BOOLEAN NOT NULL,
    discovered_at TIMESTAMP NOT NULL,
    modified_at TIMESTAMP NOT NULL,
    CONSTRAINT u_data_asset_meta_data_path_tenant UNIQUE (metadata_path, tenant)
);

-- status = 0 leyendo de data_asset, 1 = leyendo key_data_asset, 2 = key, 3 = business_assets_data_asset, 4 = business_assets
CREATE TABLE IF NOT EXISTS dg_metadata.partial_indexation_state (
    id SMALLINT NOT NULL UNIQUE PRIMARY KEY,
    last_read_data_asset TIMESTAMP,
    last_read_key_data_asset TIMESTAMP,
    last_read_key TIMESTAMP,
    last_read_business_assets_data_asset TIMESTAMP,
    last_read_business_assets TIMESTAMP
);

-- status = 0 leyendo de data_asset, 1 = leyendo key_data_asset, 2 = key, 3 = business_assets_data_asset, 4 = business_assets
CREATE TABLE IF NOT EXISTS dg_metadata.total_indexation_state (
    id SMALLINT NOT NULL UNIQUE PRIMARY KEY,
    last_read_data_asset TIMESTAMP
);

CREATE TABLE IF NOT EXISTS dg_metadata.key (
    id SERIAL NOT NULL UNIQUE PRIMARY KEY,
    key TEXT NOT NULL,
    description TEXT,
    status BOOLEAN NOT NULL,
    tenant TEXT NOT NULL,
    modified_at TIMESTAMP NOT NULL,
    CONSTRAINT u_key_key_tenant UNIQUE (key, tenant)
);

CREATE TABLE IF NOT EXISTS dg_metadata.key_data_asset (
    id SERIAL NOT NULL UNIQUE PRIMARY KEY,
    value TEXT NOT NULL,
    key_id INTEGER UNIQUE NOT NULL,
    data_asset_id INTEGER UNIQUE NOT NULL,
    tenant TEXT NOT NULL,
    modified_at TIMESTAMP NOT NULL,
    CONSTRAINT fk_key_value_key FOREIGN KEY (key_id) REFERENCES dg_metadata.key(id) ON DELETE CASCADE,
    CONSTRAINT fk_key_value_data_asset FOREIGN KEY (data_asset_id) REFERENCES dg_metadata.data_asset(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS dg_metadata.community (
    id SERIAL NOT NULL UNIQUE PRIMARY KEY,
    name TEXT NOT NULL,
    description TEXT,
    tenant TEXT NOT NULL,
    CONSTRAINT u_community_name_tenant UNIQUE (name, tenant)
);

CREATE TABLE IF NOT EXISTS dg_metadata.domain (
	  id SERIAL NOT NULL UNIQUE PRIMARY KEY,
    name TEXT NOT NULL,
    description TEXT,
    community_id INTEGER UNIQUE NOT NULL,
    tenant TEXT NOT NULL,
    CONSTRAINT u_domain_name_community_id UNIQUE (name, community_id),
    CONSTRAINT fk_domain_community FOREIGN KEY (community_id) REFERENCES dg_metadata.community(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS dg_metadata.business_assets_type (
    id SERIAL NOT NULL UNIQUE PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    description TEXT,
    properties JSONB
);

INSERT INTO dg_metadata.business_assets_type (name, description, properties) VALUES ('TERM', 'Business term', '{"description":"string","examples":"string"}');
--INSERT INTO dg_metadata.business_assets_type (name, description, properties) VALUES ('QR', 'Quality rules', '{"volumetria":"integer","regex":"string"}');

CREATE TABLE IF NOT EXISTS dg_metadata.business_assets_status (
    id SERIAL NOT NULL UNIQUE PRIMARY KEY,
    name TEXT NOT NULL,
    description TEXT
);

INSERT INTO dg_metadata.business_assets_status (description, name) VALUES ('Approved', 'APR');
INSERT INTO dg_metadata.business_assets_status (description, name) VALUES ('Pending', 'PEN');
INSERT INTO dg_metadata.business_assets_status (description, name) VALUES ('Under review', 'UNR');

CREATE TABLE IF NOT EXISTS dg_metadata.business_assets (
    id SERIAL NOT NULL UNIQUE PRIMARY KEY,
    name TEXT NOT NULL,
    description TEXT,
    properties jsonb,
    tenant TEXT NOT NULL,
    business_assets_type_id INTEGER  UNIQUE NOT NULL,
    domain_id INTEGER UNIQUE NOT NULL,
    business_assets_status_id INTEGER UNIQUE NOT NULL,
    modified_at TIMESTAMP NOT NULL,
    CONSTRAINT fk_business_assets_business_assets_type FOREIGN KEY (business_assets_type_id) REFERENCES dg_metadata.business_assets_type(id),
    CONSTRAINT fk_business_assets_domain FOREIGN KEY (domain_id) REFERENCES dg_metadata.domain(id) ON DELETE CASCADE,
    CONSTRAINT fk_business_assets_business_assets_status FOREIGN KEY (business_assets_status_id) REFERENCES dg_metadata.business_assets_status(id),
    CONSTRAINT u_business_assets_name_domain_id UNIQUE (name, domain_id)
);

CREATE TABLE IF NOT EXISTS dg_metadata.business_assets_data_asset (
    id SERIAL NOT NULL UNIQUE PRIMARY KEY,
    name TEXT NOT NULL,
    description TEXT,
    tenant TEXT NOT NULL,
    data_asset_id INTEGER UNIQUE NOT NULL,
    business_assets_id INTEGER UNIQUE NOT NULL,
    modified_at TIMESTAMP NOT NULL,
    CONSTRAINT fk_business_assets_business_assets_id_business_assets FOREIGN KEY (business_assets_id) REFERENCES dg_metadata.business_assets(id) ON DELETE CASCADE,
    CONSTRAINT fk_business_assets_data_asset_id_data_asset FOREIGN KEY (data_asset_id) REFERENCES dg_metadata.data_asset(id) ON DELETE CASCADE,
    CONSTRAINT u_business_assets_data_asset_data_asset_id_business_assets_id UNIQUE (data_asset_id, business_assets_id)
);

DROP SCHEMA dg_metadata CASCADE;
