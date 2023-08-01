CREATE SCHEMA IF NOT EXISTS gelarm;

SET search_path TO gelarm, public;

DROP TABLE gelarm.federal_projects;
CREATE TABLE IF NOT EXISTS gelarm.federal_projects (
		id SERIAL PRIMARY KEY,
		name TEXT
);

DROP TABLE gelarm.federal_organizations;
CREATE TABLE gelarm.federal_organizations (
		id SERIAL PRIMARY KEY,
		name TEXT
);

DROP TABLE gelarm.federal_projects_delayed;
CREATE TABLE gelarm.federal_projects_delayed (
		id SERIAL PRIMARY KEY,
		federal_prj_id INT4 REFERENCES gelarm.federal_projects (id) ON DELETE CASCADE,
		federal_org_id INT4 REFERENCES gelarm.federal_organizations (id) ON DELETE CASCADE,
		prj_date TIMESTAMPTZ,
		year_no	INT4,
		year_plan	INT4,
		year_achieved_cnt INT4,
		year_achieved_percent	FLOAT,
		year_left_cnt	INT4,
		year_left_percent	FLOAT,
		year_delayed_cnt INT4,
		year_delayed_percent FLOAT,
		total_delayed_cnt	INT4,
		total_delayed_percent	FLOAT,
		created_at TIMESTAMPTZ,
		updated_at TIMESTAMPTZ,
		created_from TIMESTAMPTZ,
		created_to TIMESTAMPTZ,
		relevance_dttm TIMESTAMPTZ
);
