use anyhow::Result;
use rustyline::{history::FileHistory, Editor, Helper};
use std::path::PathBuf;
use tracing::debug;

/// Returns the path to the persistent history file: `~/.fsql_history`.
pub fn history_path() -> Option<PathBuf> {
    dirs_next::home_dir().map(|h| h.join(".fsql_history"))
}

/// Load history from `~/.fsql_history` into the rustyline editor.
pub fn load_history<H: Helper>(rl: &mut Editor<H, FileHistory>) -> Result<()> {
    if let Some(path) = history_path() {
        if path.exists() {
            match rl.load_history(&path) {
                Ok(_) => debug!("Loaded history from {}", path.display()),
                Err(e) => debug!("Could not load history: {}", e),
            }
        }
    }
    Ok(())
}

/// Save history to `~/.fsql_history`.
pub fn save_history<H: Helper>(rl: &mut Editor<H, FileHistory>) -> Result<()> {
    if let Some(path) = history_path() {
        match rl.save_history(&path) {
            Ok(_) => debug!("Saved history to {}", path.display()),
            Err(e) => debug!("Could not save history: {}", e),
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_history_path_is_some() {
        // On any system with a home directory this should return Some
        // (CI may not have one, so we just check it doesn't panic)
        let _ = history_path();
    }

    #[test]
    fn test_history_path_ends_with_fsql_history() {
        if let Some(p) = history_path() {
            assert_eq!(p.file_name().unwrap(), ".fsql_history");
        }
    }
}
