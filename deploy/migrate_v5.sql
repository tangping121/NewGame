-- v5: guild persistence

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
