CREATE SCHEMA IF NOT EXISTS public;

CREATE TABLE IF NOT EXISTS public.countries (
    id SERIAL PRIMARY KEY,
    cca3 CHAR(3) UNIQUE NOT NULL,              
    name_common VARCHAR(128) NOT NULL,
    name_official VARCHAR(256),
    capital VARCHAR(128),
    region VARCHAR(64),
    subregion VARCHAR(64),
    area NUMERIC(12,2),
    population BIGINT,
    flag_png TEXT,
    flag_svg TEXT,
    languages JSONB,                          
    currencies JSONB,                          
    etl_load_date TIMESTAMPTZ DEFAULT (NOW() AT TIME ZONE 'UTC')
);

CREATE INDEX IF NOT EXISTS idx_countries_cca3 ON public.countries(cca3);
CREATE INDEX IF NOT EXISTS idx_countries_region ON public.countries(region);
CREATE INDEX IF NOT EXISTS idx_countries_population ON public.countries(population DESC);

