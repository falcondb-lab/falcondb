use std::sync::OnceLock;

static DEFAULT_TABLE_ENGINE: OnceLock<String> = OnceLock::new();
static NODE_ROLE: OnceLock<String> = OnceLock::new();

pub fn set_default_table_engine(engine: String) {
    let _ = DEFAULT_TABLE_ENGINE.set(engine);
}

pub fn default_table_engine() -> &'static str {
    DEFAULT_TABLE_ENGINE.get().map(|s| s.as_str()).unwrap_or("rowstore")
}

pub fn set_node_role(role: String) {
    let _ = NODE_ROLE.set(role);
}

pub fn node_role() -> &'static str {
    NODE_ROLE.get().map(|s| s.as_str()).unwrap_or("standalone")
}
