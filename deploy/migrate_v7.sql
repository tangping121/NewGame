-- Framework hardening: role directory/fencing, economy ledger, durable events,
-- payment verification, and persistent match/battle state.

CREATE TABLE IF NOT EXISTS role_directory (
    id          BIGINT PRIMARY KEY,
    account_id  BIGINT NOT NULL REFERENCES accounts(id),
    zone_id     INT NOT NULL,
    name        VARCHAR(32) NOT NULL,
    level       INT NOT NULL DEFAULT 1,
    shard_id    INT NOT NULL DEFAULT 0,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (account_id, zone_id),
    UNIQUE (zone_id, name)
);

INSERT INTO role_directory (id, account_id, zone_id, name, level, shard_id, created_at, updated_at)
SELECT r.id, r.account_id, r.zone_id, r.name, r.level, 0, r.created_at, r.updated_at
  FROM roles r
  JOIN accounts a ON a.id = r.account_id
ON CONFLICT (id) DO NOTHING;

ALTER TABLE roles ADD COLUMN IF NOT EXISTS version BIGINT NOT NULL DEFAULT 1;
ALTER TABLE roles ADD COLUMN IF NOT EXISTS owner_epoch BIGINT NOT NULL DEFAULT 1;
ALTER TABLE roles DROP CONSTRAINT IF EXISTS roles_account_id_fkey;

ALTER TABLE orders ADD COLUMN IF NOT EXISTS currency VARCHAR(3) NOT NULL DEFAULT 'CNY';

CREATE TABLE IF NOT EXISTS economy_ledger (
    id          BIGSERIAL PRIMARY KEY,
    role_id     BIGINT NOT NULL,
    source      VARCHAR(160) NOT NULL,
    kind        VARCHAR(32) NOT NULL,
    payload     JSONB NOT NULL DEFAULT '{}',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (role_id, source)
);
CREATE INDEX IF NOT EXISTS idx_economy_ledger_role ON economy_ledger(role_id, id DESC);

ALTER TABLE orders ADD COLUMN IF NOT EXISTS provider_transaction_id VARCHAR(128);
ALTER TABLE orders ADD COLUMN IF NOT EXISTS paid_at TIMESTAMPTZ;
ALTER TABLE orders ADD COLUMN IF NOT EXISTS delivered_at TIMESTAMPTZ;
CREATE UNIQUE INDEX IF NOT EXISTS uq_orders_provider_transaction
    ON orders(provider_transaction_id) WHERE provider_transaction_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS event_outbox (
    id             BIGSERIAL PRIMARY KEY,
    event_id       VARCHAR(64) NOT NULL UNIQUE,
    subject        VARCHAR(128) NOT NULL,
    aggregate_id   VARCHAR(128) NOT NULL DEFAULT '',
    aggregate_ver  BIGINT NOT NULL DEFAULT 0,
    payload        JSONB NOT NULL,
    published_at   TIMESTAMPTZ,
    attempts       INT NOT NULL DEFAULT 0,
    available_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_event_outbox_pending
    ON event_outbox(available_at, id) WHERE published_at IS NULL;

CREATE TABLE IF NOT EXISTS event_inbox (
    consumer       VARCHAR(128) NOT NULL,
    event_id       VARCHAR(64) NOT NULL,
    processed_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (consumer, event_id)
);

CREATE TABLE IF NOT EXISTS match_tickets (
    id          VARCHAR(64) PRIMARY KEY,
    role_id     BIGINT NOT NULL,
    zone_id     INT NOT NULL,
    mode        INT NOT NULL,
    state       VARCHAR(24) NOT NULL,
    lease_until TIMESTAMPTZ,
    match_id    VARCHAR(64),
    room_id     VARCHAR(64),
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_match_ticket_queue
    ON match_tickets(mode, created_at) WHERE state = 'QUEUED';
CREATE UNIQUE INDEX IF NOT EXISTS uq_match_ticket_active_role
    ON match_tickets(role_id, mode) WHERE state IN ('QUEUED', 'RESERVED');

CREATE TABLE IF NOT EXISTS match_rooms (
    id          VARCHAR(64) PRIMARY KEY,
    battle_room_id VARCHAR(64),
    members     JSONB NOT NULL,
    state       VARCHAR(24) NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS battle_rooms (
    id          VARCHAR(64) PRIMARY KEY,
    members     JSONB NOT NULL,
    state       VARCHAR(24) NOT NULL DEFAULT 'ACTIVE',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at  TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS battle_results (
    room_id     VARCHAR(64) NOT NULL REFERENCES battle_rooms(id) ON DELETE CASCADE,
    role_id     BIGINT NOT NULL,
    result      JSONB NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (room_id, role_id)
);

ALTER TABLE auction_listings ADD COLUMN IF NOT EXISTS request_key VARCHAR(128);
ALTER TABLE auction_listings ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW();
CREATE UNIQUE INDEX IF NOT EXISTS uq_auction_request_key
    ON auction_listings(request_key) WHERE request_key IS NOT NULL;
