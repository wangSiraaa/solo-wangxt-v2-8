-- H2 (MODE=MySQL) equivalent of schema-mysql.sql, used for local run & unit tests.
CREATE TABLE IF NOT EXISTS app_user (
    id            BIGINT       NOT NULL AUTO_INCREMENT,
    username      VARCHAR(64)  NOT NULL,
    password_hash VARCHAR(100) NOT NULL,
    role          VARCHAR(16)  NOT NULL,
    display_name  VARCHAR(64)  NOT NULL,
    created_at    TIMESTAMP(3) NOT NULL,
    PRIMARY KEY (id),
    CONSTRAINT uk_user_username UNIQUE (username)
);

CREATE TABLE IF NOT EXISTS login_token (
    token      CHAR(48)    NOT NULL,
    user_id    BIGINT      NOT NULL,
    role       VARCHAR(16) NOT NULL,
    expires_at TIMESTAMP(3) NOT NULL,
    created_at TIMESTAMP(3) NOT NULL,
    PRIMARY KEY (token)
);
CREATE INDEX IF NOT EXISTS ix_token_user ON login_token(user_id);

CREATE TABLE IF NOT EXISTS recipe (
    id          BIGINT      NOT NULL AUTO_INCREMENT,
    code        VARCHAR(64) NOT NULL,
    name        VARCHAR(128) NOT NULL,
    status      VARCHAR(16) NOT NULL,
    created_by  BIGINT      NOT NULL,
    created_at  TIMESTAMP(3) NOT NULL,
    updated_at  TIMESTAMP(3) NOT NULL,
    PRIMARY KEY (id),
    CONSTRAINT uk_recipe_code UNIQUE (code)
);

CREATE TABLE IF NOT EXISTS recipe_version (
    id              BIGINT       NOT NULL AUTO_INCREMENT,
    recipe_id       BIGINT       NOT NULL,
    version_no      INT          NOT NULL,
    status          VARCHAR(16)  NOT NULL,
    inputs_json     CLOB         NULL,
    outputs_json    CLOB         NULL,
    start_time      TIMESTAMP(3) NULL,
    end_time        TIMESTAMP(3) NULL,
    craft_timeout_s INT          NOT NULL,
    published_at    TIMESTAMP(3) NULL,
    published_by    BIGINT       NULL,
    recalled        TINYINT      NOT NULL DEFAULT 0,
    recalled_at     TIMESTAMP(3) NULL,
    recall_batch_no CHAR(20)     NULL,
    created_at      TIMESTAMP(3) NOT NULL,
    PRIMARY KEY (id),
    CONSTRAINT uk_recipe_version UNIQUE (recipe_id, version_no)
);
CREATE INDEX IF NOT EXISTS ix_rv_published_lookup ON recipe_version(recipe_id, status);

CREATE TABLE IF NOT EXISTS player_inventory (
    player_id  BIGINT      NOT NULL,
    item_code  VARCHAR(64) NOT NULL,
    qty        BIGINT      NOT NULL,
    updated_at TIMESTAMP(3) NOT NULL,
    PRIMARY KEY (player_id, item_code),
    CONSTRAINT ck_inv_qty_nonneg CHECK (qty >= 0)
);

CREATE TABLE IF NOT EXISTS craft_order (
    id                  BIGINT       NOT NULL AUTO_INCREMENT,
    order_no            CHAR(20)     NOT NULL,
    player_id           BIGINT       NOT NULL,
    recipe_id           BIGINT       NOT NULL,
    recipe_version_id   BIGINT       NOT NULL,
    status              VARCHAR(16)  NOT NULL,
    status_reason       VARCHAR(255) NULL,
    preoccupy_deadline  TIMESTAMP(3) NOT NULL,
    committed_at        TIMESTAMP(3) NULL,
    closed_at           TIMESTAMP(3) NULL,
    revoke_ref_no       CHAR(20)     NULL,
    recall_batch_no     CHAR(20)     NULL,
    recall_settled_at   TIMESTAMP(3) NULL,
    created_at          TIMESTAMP(3) NOT NULL,
    PRIMARY KEY (id),
    CONSTRAINT uk_order_no UNIQUE (order_no)
);
CREATE INDEX IF NOT EXISTS ix_order_player ON craft_order(player_id, created_at);
CREATE INDEX IF NOT EXISTS ix_order_status_deadline ON craft_order(status, preoccupy_deadline);

CREATE TABLE IF NOT EXISTS material_hold (
    id           BIGINT      NOT NULL AUTO_INCREMENT,
    order_id     BIGINT      NOT NULL,
    player_id    BIGINT      NOT NULL,
    item_code    VARCHAR(64) NOT NULL,
    qty          BIGINT      NOT NULL,
    released     TINYINT     NOT NULL DEFAULT 0,
    created_at   TIMESTAMP(3) NOT NULL,
    released_at  TIMESTAMP(3) NULL,
    PRIMARY KEY (id),
    CONSTRAINT uk_hold_order_item UNIQUE (order_id, item_code)
);
CREATE INDEX IF NOT EXISTS ix_hold_player ON material_hold(player_id, released);

CREATE TABLE IF NOT EXISTS ledger_entry (
    id            BIGINT       NOT NULL AUTO_INCREMENT,
    ref_no        CHAR(20)     NOT NULL,
    player_id     BIGINT       NOT NULL,
    item_code     VARCHAR(64)  NOT NULL,
    entry_type    VARCHAR(24)  NOT NULL,
    qty_delta     BIGINT      NOT NULL,
    related_ref   CHAR(20)     NULL,
    status        VARCHAR(16)  NOT NULL DEFAULT 'POSTED',
    remark        VARCHAR(255) NULL,
    created_at    TIMESTAMP(3) NOT NULL,
    PRIMARY KEY (id),
    CONSTRAINT uk_ledger_ref_player_item_type UNIQUE (ref_no, player_id, item_code, entry_type)
);
CREATE INDEX IF NOT EXISTS ix_ledger_player ON ledger_entry(player_id, created_at);
CREATE INDEX IF NOT EXISTS ix_ledger_related ON ledger_entry(related_ref);

CREATE TABLE IF NOT EXISTS idempotency_record (
    idempotency_key VARCHAR(80)  NOT NULL,
    player_id       BIGINT       NOT NULL,
    scope           VARCHAR(32)  NOT NULL,
    ref_no          CHAR(20)     NOT NULL,
    response_json   CLOB         NOT NULL,
    status          VARCHAR(16)  NOT NULL,
    created_at      TIMESTAMP(3) NOT NULL,
    updated_at      TIMESTAMP(3) NOT NULL,
    PRIMARY KEY (idempotency_key)
);
CREATE INDEX IF NOT EXISTS ix_idem_player ON idempotency_record(player_id);

CREATE TABLE IF NOT EXISTS revoke_record (
    id               BIGINT       NOT NULL AUTO_INCREMENT,
    revoke_no        CHAR(20)     NOT NULL,
    order_id         BIGINT       NOT NULL,
    order_no         CHAR(20)     NOT NULL,
    player_id        BIGINT       NOT NULL,
    result           VARCHAR(16)  NOT NULL,
    shortage_json    CLOB         NULL,
    operator_id      BIGINT       NOT NULL,
    created_at       TIMESTAMP(3) NOT NULL,
    PRIMARY KEY (id),
    CONSTRAINT uk_revoke_order UNIQUE (order_id),
    CONSTRAINT uk_revoke_no UNIQUE (revoke_no)
);
CREATE INDEX IF NOT EXISTS ix_revoke_result ON revoke_record(result);

-- ============================================================================
-- 版本紧急召回与清算批次
-- 一个被召回版本至多一个批次（uk_recall_batch_version）；同一幂等键只返回原批次。
-- ============================================================================
CREATE TABLE IF NOT EXISTS recall_batch (
    id               BIGINT       NOT NULL AUTO_INCREMENT,
    batch_no         CHAR(20)     NOT NULL,
    idempotency_key  VARCHAR(80)  NOT NULL,
    recipe_id        BIGINT       NOT NULL,
    recipe_version_id BIGINT      NOT NULL,
    version_no       INT          NOT NULL,
    cutoff_at        TIMESTAMP(3) NOT NULL,
    status           VARCHAR(24)  NOT NULL,
    reason           VARCHAR(255) NULL,
    operator_id      BIGINT       NOT NULL,
    total_orders     INT          NOT NULL DEFAULT 0,
    processed_orders INT          NOT NULL DEFAULT 0,
    result_released  INT          NOT NULL DEFAULT 0,
    result_reversed  INT          NOT NULL DEFAULT 0,
    result_skipped   INT          NOT NULL DEFAULT 0,
    result_exception INT          NOT NULL DEFAULT 0,
    created_at       TIMESTAMP(3) NOT NULL,
    started_at       TIMESTAMP(3) NULL,
    finished_at      TIMESTAMP(3) NULL,
    PRIMARY KEY (id),
    CONSTRAINT uk_recall_batch_no UNIQUE (batch_no),
    CONSTRAINT uk_recall_batch_key UNIQUE (idempotency_key),
    CONSTRAINT uk_recall_batch_version UNIQUE (recipe_version_id)
);
CREATE INDEX IF NOT EXISTS ix_recall_batch_status ON recall_batch(status);

-- 逐单清算结果（批次快照的一部分）：每单在每批次中恰好一行，结果唯一归依。
CREATE TABLE IF NOT EXISTS recall_batch_order (
    id               BIGINT       NOT NULL AUTO_INCREMENT,
    batch_id         BIGINT       NOT NULL,
    batch_no         CHAR(20)     NOT NULL,
    order_id         BIGINT       NOT NULL,
    order_no         CHAR(20)     NOT NULL,
    player_id        BIGINT       NOT NULL,
    snapshot_status  VARCHAR(16)  NOT NULL,
    status           VARCHAR(16)  NOT NULL,
    result           VARCHAR(24)  NULL,
    detail_json      CLOB         NULL,
    attempts         INT          NOT NULL DEFAULT 0,
    settled_at       TIMESTAMP(3) NULL,
    created_at       TIMESTAMP(3) NOT NULL,
    updated_at       TIMESTAMP(3) NOT NULL,
    PRIMARY KEY (id),
    CONSTRAINT uk_recall_order UNIQUE (batch_id, order_id)
);
CREATE INDEX IF NOT EXISTS ix_recall_order_status ON recall_batch_order(batch_id, status);
CREATE INDEX IF NOT EXISTS ix_recall_order_result ON recall_batch_order(batch_id, result);

-- 逐次处理尝试（含崩溃前的未决尝试与可重试异常历史，全程可追溯，前次记录保留）。
CREATE TABLE IF NOT EXISTS recall_batch_event (
    id          BIGINT       NOT NULL AUTO_INCREMENT,
    batch_no    CHAR(20)     NOT NULL,
    order_id    BIGINT       NOT NULL,
    order_no    CHAR(20)     NOT NULL,
    event_type  VARCHAR(24)  NOT NULL,
    from_status VARCHAR(16)  NULL,
    to_status   VARCHAR(16)  NULL,
    result      VARCHAR(24)  NULL,
    message     VARCHAR(500) NULL,
    created_at  TIMESTAMP(3) NOT NULL,
    PRIMARY KEY (id)
);
CREATE INDEX IF NOT EXISTS ix_recall_event_order ON recall_batch_event(batch_no, order_id);
