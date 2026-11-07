-- Saga de pedido (processes/bpmn/pedido-saga.bpmn): pedidos, estoque, reservas e pagamentos.

CREATE TABLE IF NOT EXISTS orders (
    id                   TEXT PRIMARY KEY,
    customer             TEXT        NOT NULL,
    items                JSONB       NOT NULL,
    total_cents          BIGINT      NOT NULL CHECK (total_cents >= 0),
    status               TEXT        NOT NULL,
    rejection_reason     TEXT        NOT NULL DEFAULT '',
    process_instance_key TEXT        NOT NULL DEFAULT '',
    created_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at           TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS orders_status_idx ON orders (status, created_at DESC);

CREATE TABLE IF NOT EXISTS inventory_items (
    sku       TEXT PRIMARY KEY,
    name      TEXT NOT NULL,
    available INT  NOT NULL CHECK (available >= 0)
);

CREATE TABLE IF NOT EXISTS inventory_reservations (
    order_id   TEXT        NOT NULL,
    sku        TEXT        NOT NULL REFERENCES inventory_items (sku),
    quantity   INT         NOT NULL CHECK (quantity > 0),
    status     TEXT        NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (order_id, sku)
);

CREATE TABLE IF NOT EXISTS payments (
    id             TEXT PRIMARY KEY,
    order_id       TEXT        NOT NULL UNIQUE,
    amount_cents   BIGINT      NOT NULL,
    status         TEXT        NOT NULL,
    transaction_id TEXT        NOT NULL DEFAULT '',
    decline_reason TEXT        NOT NULL DEFAULT '',
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Estoque inicial para estudo (ajuste com PUT /api/v1/inventory/:sku)
INSERT INTO inventory_items (sku, name, available) VALUES
    ('NOTEBOOK', 'Notebook 14"', 10),
    ('MONITOR',  'Monitor 27"',   5),
    ('TECLADO',  'Teclado mecânico', 50),
    ('MOUSE',    'Mouse sem fio', 100)
ON CONFLICT (sku) DO NOTHING;
