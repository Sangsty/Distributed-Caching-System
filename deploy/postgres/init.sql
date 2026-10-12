-- Runs once, when the database volume is first created
-- (docker-entrypoint-initdb.d). Keep in sync with catalog/postgres.go and
-- catalog.MemoryRepo.Seed.
CREATE TABLE IF NOT EXISTS products (
    id          BIGSERIAL   PRIMARY KEY,
    name        TEXT        NOT NULL CHECK (length(name) BETWEEN 1 AND 200),
    description TEXT        NOT NULL DEFAULT '',
    price_cents BIGINT      NOT NULL CHECK (price_cents >= 0),
    stock       INTEGER     NOT NULL DEFAULT 0 CHECK (stock >= 0),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

INSERT INTO products (name, description, price_cents, stock)
SELECT 'Product ' || g,
       'Description of product ' || g,
       100 + (g * 37) % 9900,
       g % 50
FROM generate_series(1, 1000) AS g;
