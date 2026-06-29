-- v2: mail, social, activity persistence

CREATE TABLE IF NOT EXISTS mails (
    id          BIGSERIAL PRIMARY KEY,
    role_id     BIGINT NOT NULL,
    title       VARCHAR(128) NOT NULL DEFAULT '',
    content     TEXT NOT NULL DEFAULT '',
    items       VARCHAR(256) NOT NULL DEFAULT '',
    is_read     BOOLEAN NOT NULL DEFAULT FALSE,
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
