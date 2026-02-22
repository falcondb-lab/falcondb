/// Output mode for query results.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum OutputMode {
    #[default]
    Table,
    Expanded,
    TuplesOnly,
    Csv,
    Json,
}

/// Format a command tag for display.
pub fn format_command_tag(tag: &str) -> String {
    tag.to_string()
}
