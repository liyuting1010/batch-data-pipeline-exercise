default_end_time = '2999-12-31 23:59:59'

create_stg_inventory_snapshot_sql = """
CREATE TABLE IF NOT EXISTS stg_inventory_snapshot (
    productId VARCHAR NOT NULL,
    amount DECIMAL,
    date timestamp
);

truncate stg_inventory_snapshot;
"""
