use anyhow::Result;
use serde_json::json;

use crate::store::Store;

pub async fn print_events(
    store: &Store,
    run_id: Option<&str>,
    entity_type: Option<&str>,
    entity_id: Option<&str>,
    limit: i64,
) -> Result<()> {
    let rows = store
        .list_events(run_id, entity_type, entity_id, limit)
        .await?;

    for row in rows {
        let output = json!({
            "event_id": row.event_id,
            "run_id": row.run_id,
            "source": row.source,
            "action": row.action,
            "entity_type": row.entity_type,
            "entity_id": row.entity_id,
            "before_json": row.before_json,
            "after_json": row.after_json,
            "meta_json": row.meta_json,
            "occurred_at": row.occurred_at,
        });
        println!("{}", serde_json::to_string(&output)?);
    }

    Ok(())
}
