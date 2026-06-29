-- NewGame initial schema (player snapshot + account)

CREATE TABLE IF NOT EXISTS accounts (
    id          BIGSERIAL PRIMARY KEY,
    username    VARCHAR(64) NOT NULL UNIQUE,
    password_hash VARCHAR(128) NOT NULL DEFAULT '',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS roles (
    id          BIGINT PRIMARY KEY,
    account_id  BIGINT NOT NULL REFERENCES accounts(id),
    zone_id     INT NOT NULL,
    name        VARCHAR(32) NOT NULL,
    level       INT NOT NULL DEFAULT 1,
    snapshot    JSONB NOT NULL DEFAULT '{}',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (zone_id, name)
);

CREATE TABLE IF NOT EXISTS orders (
    id          VARCHAR(64) PRIMARY KEY,
    role_id     BIGINT NOT NULL,
    product_id  VARCHAR(64) NOT NULL,
    amount      INT NOT NULL,
    status      SMALLINT NOT NULL DEFAULT 0,
    delivered   BOOLEAN NOT NULL DEFAULT FALSE,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_roles_account ON roles(account_id);
CREATE INDEX IF NOT EXISTS idx_orders_role ON orders(role_id);

CREATE TABLE IF NOT EXISTS mails (
    id          BIGSERIAL PRIMARY KEY,
    role_id     BIGINT NOT NULL,
    title       VARCHAR(128) NOT NULL DEFAULT '',
    content     TEXT NOT NULL DEFAULT '',
    items       VARCHAR(256) NOT NULL DEFAULT '',
    is_read     BOOLEAN NOT NULL DEFAULT FALSE,
    claimed     BOOLEAN NOT NULL DEFAULT FALSE,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS chat_messages (
    id          BIGSERIAL PRIMARY KEY,
    channel     VARCHAR(32) NOT NULL DEFAULT 'world',
    role_id     BIGINT NOT NULL,
    text        TEXT NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS friends (
    role_id     BIGINT NOT NULL,
    friend_id   BIGINT NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (role_id, friend_id)
);

CREATE TABLE IF NOT EXISTS activity_progress (
    role_id     BIGINT NOT NULL,
    activity_id INT NOT NULL,
    progress    INT NOT NULL DEFAULT 0,
    claimed     BOOLEAN NOT NULL DEFAULT FALSE,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (role_id, activity_id)
);

CREATE INDEX IF NOT EXISTS idx_mails_role ON mails(role_id);
CREATE INDEX IF NOT EXISTS idx_chat_channel ON chat_messages(channel, created_at DESC);

CREATE TABLE IF NOT EXISTS guilds (
    id          BIGSERIAL PRIMARY KEY,
    zone_id     INT NOT NULL,
    name        VARCHAR(64) NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (zone_id, name)
);

CREATE TABLE IF NOT EXISTS guild_members (
    guild_id    BIGINT NOT NULL REFERENCES guilds(id) ON DELETE CASCADE,
    role_id     BIGINT NOT NULL,
    joined_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (guild_id, role_id)
);

CREATE INDEX IF NOT EXISTS idx_guild_members_role ON guild_members(role_id);

INSERT INTO guilds (id, zone_id, name)
VALUES (1, 1, 'default_guild')
ON CONFLICT DO NOTHING;

CREATE TABLE IF NOT EXISTS auction_listings (
    id              BIGSERIAL PRIMARY KEY,
    seller_role_id  BIGINT NOT NULL,
    item_id         VARCHAR(64) NOT NULL,
    qty             INT NOT NULL,
    price           BIGINT NOT NULL,
    status          SMALLINT NOT NULL DEFAULT 0,
    buyer_role_id   BIGINT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_auction_open ON auction_listings(status, created_at DESC);
