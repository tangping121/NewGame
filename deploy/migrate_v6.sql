-- Auction house listings

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
