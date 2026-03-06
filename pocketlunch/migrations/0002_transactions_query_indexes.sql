CREATE INDEX IF NOT EXISTS idx_lm_transactions_date_id_desc
    ON lm_transactions(date DESC, id DESC);

CREATE INDEX IF NOT EXISTS idx_lm_transactions_category_date_id_desc
    ON lm_transactions(category_id, date DESC, id DESC);
