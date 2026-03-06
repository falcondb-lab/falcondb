use std::cell::RefCell;
use std::collections::HashMap;

use falcon_common::datum::Datum;
use falcon_common::error::SqlError;
use falcon_common::schema::{Catalog, ColumnDef, TableSchema};
use falcon_common::types::{ColumnId, DataType, TableId};
use sqlparser::ast::{self, Expr, SetExpr, Statement, Value};

use crate::param_env::ParamEnv;
use crate::types::*;
use crate::var_registry::VarRegistry;

/// Alias map: maps alias/table name -> (real_table_name, column_offset_in_combined_schema).
pub(crate) type AliasMap = HashMap<String, (String, usize)>;

/// Result of binding a statement with parameter type inference.
#[derive(Debug, Clone)]
pub struct BindResult {
    pub statement: BoundStatement,
    /// Inferred types for each parameter ($1..$n), in order.
    pub param_types: Vec<DataType>,
    /// Number of parameters found.
    pub param_count: usize,
}

/// The binder resolves names and types against the catalog, producing BoundStatements.
pub struct Binder {
    pub(crate) catalog: Catalog,
    pub(crate) next_table_id: u64,
    /// Parameter type inference environment (populated during bind_with_params).
    /// Uses RefCell for interior mutability so bind_expr can stay &self.
    pub(crate) param_env: RefCell<Option<ParamEnv>>,
    /// SHOW variable registry — decoupled from binder match logic.
    var_registry: VarRegistry,
}

impl Binder {
    pub fn new(catalog: Catalog) -> Self {
        let next_id = catalog
            .tables_map()
            .values()
            .map(|t| t.id.0)
            .max()
            .unwrap_or(0)
            + 1;
        Self {
            catalog,
            next_table_id: next_id,
            param_env: RefCell::new(None),
            var_registry: VarRegistry::with_defaults(),
        }
    }

    fn default_storage_type() -> falcon_common::schema::StorageType {
        match falcon_common::globals::default_table_engine() {
            "columnstore" => falcon_common::schema::StorageType::Columnstore,
            "disk" | "disk_rowstore" | "diskrowstore" => falcon_common::schema::StorageType::DiskRowstore,
            "lsm" | "lsm_rowstore" | "lsmrowstore" => falcon_common::schema::StorageType::LsmRowstore,
            "rocksdb" | "rocks" => falcon_common::schema::StorageType::RocksDbRowstore,
            "redb" => falcon_common::schema::StorageType::RedbRowstore,
            _ => falcon_common::schema::StorageType::Rowstore,
        }
    }

    /// Bind a statement with parameter type inference.
    /// Returns a `BindResult` containing the bound statement and inferred param types.
    /// If `type_hints` is provided, those types seed the ParamEnv (from PG Parse message).
    pub fn bind_with_params(
        &mut self,
        stmt: &Statement,
        type_hints: Option<&[DataType]>,
    ) -> Result<BindResult, SqlError> {
        *self.param_env.borrow_mut() = Some(type_hints.map_or_else(ParamEnv::new, ParamEnv::with_type_hints));
        let bound = self.bind(stmt);
        let env = self.param_env.borrow_mut().take().ok_or_else(|| {
            SqlError::InternalInvariant("param_env was not set before bind_with_params".into())
        })?;
        let bound = bound?;
        let param_types = env.finalize()?;
        let param_count = param_types.len();
        Ok(BindResult {
            statement: bound,
            param_types,
            param_count,
        })
    }

    /// Bind a statement with parameter type inference, allowing unresolved params.
    /// Used when parameter types cannot be fully inferred (e.g. `SELECT $1`).
    /// Returns best-effort types with None for unresolved params.
    pub fn bind_with_params_lenient(
        &mut self,
        stmt: &Statement,
        type_hints: Option<&[DataType]>,
    ) -> Result<(BoundStatement, Vec<Option<DataType>>), SqlError> {
        *self.param_env.borrow_mut() = Some(type_hints.map_or_else(ParamEnv::new, ParamEnv::with_type_hints));
        let bound = self.bind(stmt);
        let env = self.param_env.borrow_mut().take().ok_or_else(|| {
            SqlError::InternalInvariant(
                "param_env was not set before bind_with_params_lenient".into(),
            )
        })?;
        let bound = bound?;
        let types = env.types().to_vec();
        Ok((bound, types))
    }

    pub fn bind(&mut self, stmt: &Statement) -> Result<BoundStatement, SqlError> {
        match stmt {
            Statement::CreateDatabase { db_name, if_not_exists, .. } => {
                let name = db_name.to_string();
                Ok(BoundStatement::CreateDatabase {
                    name,
                    if_not_exists: *if_not_exists,
                })
            }
            Statement::CreateTable(create) => self.bind_create_table(create),
            Statement::Drop {
                object_type,
                names,
                if_exists,
                ..
            } => match object_type {
                ast::ObjectType::Schema => {
                    let name = names
                        .first()
                        .ok_or_else(|| SqlError::Parse("DROP DATABASE requires a name".into()))?;
                    Ok(BoundStatement::DropDatabase {
                        name: name.to_string(),
                        if_exists: *if_exists,
                    })
                }
                ast::ObjectType::Table => {
                    let name = names
                        .first()
                        .ok_or_else(|| SqlError::Parse("DROP TABLE requires a name".into()))?;
                    Ok(BoundStatement::DropTable(BoundDropTable {
                        table_name: name.to_string(),
                        if_exists: *if_exists,
                    }))
                }
                ast::ObjectType::Index => {
                    let name = names
                        .first()
                        .ok_or_else(|| SqlError::Parse("DROP INDEX requires a name".into()))?;
                    Ok(BoundStatement::DropIndex {
                        index_name: name.to_string(),
                    })
                }
                ast::ObjectType::View => {
                    let name = names
                        .first()
                        .ok_or_else(|| SqlError::Parse("DROP VIEW requires a name".into()))?;
                    let name_str = name.to_string();
                    if self.catalog.find_materialized_view(&name_str).is_some() {
                        Ok(BoundStatement::DropMaterializedView {
                            name: name_str,
                            if_exists: *if_exists,
                        })
                    } else {
                        Ok(BoundStatement::DropView {
                            name: name_str,
                            if_exists: *if_exists,
                        })
                    }
                }
                ast::ObjectType::Sequence => {
                    let name = names
                        .first()
                        .ok_or_else(|| SqlError::Parse("DROP SEQUENCE requires a name".into()))?;
                    Ok(BoundStatement::DropSequence {
                        name: name.to_string(),
                        if_exists: *if_exists,
                    })
                }
                ast::ObjectType::Role => {
                    let name = names
                        .first()
                        .ok_or_else(|| SqlError::Parse("DROP ROLE requires a name".into()))?;
                    Ok(BoundStatement::DropRole {
                        name: name.to_string(),
                        if_exists: *if_exists,
                    })
                }
                _ => Err(SqlError::Unsupported(format!("DROP {object_type:?}"))),
            },
            Statement::AlterTable {
                operations, name, ..
            } => self.bind_alter_table(name, operations),
            Statement::Insert(insert) => self.bind_insert(insert),
            Statement::Update {
                table,
                assignments,
                selection,
                returning,
                from,
                ..
            } => self.bind_update(table, assignments, selection, returning, from),
            Statement::Delete(delete) => self.bind_delete(delete),
            Statement::Merge { table, source, on, clauses, .. } => {
                self.bind_merge(table, source, on, clauses)
            }
            Statement::Query(query) => self.bind_select(query),
            Statement::Explain {
                statement, analyze, ..
            } => {
                let inner = self.bind(statement)?;
                if *analyze {
                    Ok(BoundStatement::ExplainAnalyze(Box::new(inner)))
                } else {
                    Ok(BoundStatement::Explain(Box::new(inner)))
                }
            }
            Statement::Truncate { table_name, .. } => Ok(BoundStatement::Truncate {
                table_name: table_name.to_string(),
            }),
            Statement::CreateIndex(create_idx) => {
                let table_name = create_idx.table_name.to_string();
                let schema = self
                    .catalog
                    .find_table(&table_name)
                    .ok_or_else(|| SqlError::UnknownTable(table_name.clone()))?;
                let mut column_indices = Vec::new();
                for col_expr in &create_idx.columns {
                    let col_name = format!("{}", col_expr.expr);
                    let idx = schema
                        .find_column(&col_name)
                        .ok_or(SqlError::UnknownColumn(col_name))?;
                    column_indices.push(idx);
                }
                let index_name = create_idx
                    .name
                    .as_ref()
                    .map(std::string::ToString::to_string)
                    .unwrap_or_else(|| {
                        format!(
                            "idx_{}_{}",
                            table_name,
                            column_indices
                                .iter()
                                .map(std::string::ToString::to_string)
                                .collect::<Vec<_>>()
                                .join("_")
                        )
                    });
                Ok(BoundStatement::CreateIndex {
                    index_name,
                    table_name,
                    column_indices,
                    unique: create_idx.unique,
                    concurrently: create_idx.concurrently,
                })
            }
            Statement::StartTransaction { .. } => Ok(BoundStatement::Begin),
            Statement::Commit { .. } => Ok(BoundStatement::Commit),
            Statement::Rollback { .. } => Ok(BoundStatement::Rollback),
            Statement::ShowVariable { variable } => {
                let var_name = variable
                    .iter()
                    .map(|id| id.value.to_lowercase())
                    .collect::<Vec<_>>()
                    .join("_");
                match self.var_registry.resolve(&var_name) {
                    Some(stmt) => Ok(stmt),
                    None => Err(SqlError::Unsupported(format!("SHOW {var_name}"))),
                }
            }
            Statement::Analyze { table_name, .. } => {
                let name = table_name.to_string();
                Ok(BoundStatement::Analyze { table_name: name })
            }
            Statement::CreateView {
                name,
                query,
                or_replace,
                materialized,
                ..
            } => {
                let view_name = name.to_string();
                let query_sql = format!("{query}");
                if *materialized {
                    Ok(BoundStatement::CreateMaterializedView {
                        name: view_name,
                        query_sql,
                    })
                } else {
                    Ok(BoundStatement::CreateView {
                        name: view_name,
                        query_sql,
                        or_replace: *or_replace,
                    })
                }
            }
            Statement::CreateSequence {
                name,
                sequence_options,
                ..
            } => {
                let seq_name = name.to_string().to_lowercase();
                let start = sequence_options
                    .iter()
                    .find_map(|opt| {
                        if let ast::SequenceOptions::StartWith(
                            Expr::Value(Value::Number(n, _)),
                            _,
                        ) = opt
                        {
                            n.parse::<i64>().ok()
                        } else {
                            None
                        }
                    })
                    .unwrap_or(1);
                Ok(BoundStatement::CreateSequence {
                    name: seq_name,
                    start,
                })
            }
            Statement::Copy {
                source,
                to,
                target,
                options,
                legacy_options,
                ..
            } => self.bind_copy(source, *to, target, options, legacy_options),
            Statement::CreateRole {
                names,
                login,
                superuser,
                create_db,
                create_role,
                password,
                ..
            } => {
                let name = names
                    .first()
                    .map(std::string::ToString::to_string)
                    .unwrap_or_default();
                let pw = match password {
                    Some(ast::Password::Password(expr)) => Some(format!("{expr}")),
                    _ => None,
                };
                Ok(BoundStatement::CreateRole {
                    name,
                    can_login: login.unwrap_or(false),
                    is_superuser: superuser.unwrap_or(false),
                    can_create_db: create_db.unwrap_or(false),
                    can_create_role: create_role.unwrap_or(false),
                    password: pw,
                })
            }
            Statement::AlterRole { name, operation, .. } => {
                let role_name = name.value.clone();
                let mut pwd: Option<Option<String>> = None;
                let mut al_login: Option<bool> = None;
                let mut al_superuser: Option<bool> = None;
                let mut al_createdb: Option<bool> = None;
                let mut al_createrole: Option<bool> = None;
                if let ast::AlterRoleOperation::RenameRole { .. } = operation {
                    return Err(SqlError::Unsupported("ALTER ROLE RENAME".into()));
                }
                if let ast::AlterRoleOperation::WithOptions { options } = operation {
                    for opt in options {
                        match opt {
                            ast::RoleOption::Login(b) => al_login = Some(*b),
                            ast::RoleOption::SuperUser(b) => al_superuser = Some(*b),
                            ast::RoleOption::CreateDB(b) => al_createdb = Some(*b),
                            ast::RoleOption::CreateRole(b) => al_createrole = Some(*b),
                            ast::RoleOption::Password(p) => {
                                pwd = Some(match p {
                                    ast::Password::Password(expr) => Some(format!("{expr}")),
                                    ast::Password::NullPassword => None,
                                });
                            }
                            _ => {}
                        }
                    }
                }
                Ok(BoundStatement::AlterRole {
                    name: role_name,
                    password: pwd,
                    can_login: al_login,
                    is_superuser: al_superuser,
                    can_create_db: al_createdb,
                    can_create_role: al_createrole,
                })
            }
            Statement::Grant {
                privileges,
                objects,
                grantees,
                ..
            } => {
                let priv_str = match privileges {
                    ast::Privileges::All { .. } => "ALL".to_owned(),
                    ast::Privileges::Actions(actions) => {
                        actions.iter().map(|a| format!("{a}")).collect::<Vec<_>>().join(", ")
                    }
                };
                let (obj_type, obj_name) = match objects {
                    ast::GrantObjects::Tables(names) => {
                        let name = names.first().map(std::string::ToString::to_string).unwrap_or_default();
                        ("TABLE".to_owned(), name)
                    }
                    ast::GrantObjects::Schemas(names) => {
                        let name = names.first().map(std::string::ToString::to_string).unwrap_or_default();
                        ("SCHEMA".to_owned(), name)
                    }
                    ast::GrantObjects::Sequences(names) => {
                        let name = names.first().map(std::string::ToString::to_string).unwrap_or_default();
                        ("SEQUENCE".to_owned(), name)
                    }
                    ast::GrantObjects::AllTablesInSchema { schemas } => {
                        let name = schemas.first().map(std::string::ToString::to_string).unwrap_or_default();
                        ("SCHEMA".to_owned(), name)
                    }
                    _ => {
                        return Err(SqlError::Unsupported("GRANT on this object type".into()));
                    }
                };
                let grantee = grantees
                    .first()
                    .map(|g| g.value.clone())
                    .unwrap_or_default();
                Ok(BoundStatement::Grant {
                    privilege: priv_str,
                    object_type: obj_type,
                    object_name: obj_name,
                    grantee,
                })
            }
            Statement::Revoke {
                privileges,
                objects,
                grantees,
                ..
            } => {
                let priv_str = match privileges {
                    ast::Privileges::All { .. } => "ALL".to_owned(),
                    ast::Privileges::Actions(actions) => {
                        actions.iter().map(|a| format!("{a}")).collect::<Vec<_>>().join(", ")
                    }
                };
                let (obj_type, obj_name) = match objects {
                    ast::GrantObjects::Tables(names) => {
                        let name = names.first().map(std::string::ToString::to_string).unwrap_or_default();
                        ("TABLE".to_owned(), name)
                    }
                    ast::GrantObjects::Schemas(names) => {
                        let name = names.first().map(std::string::ToString::to_string).unwrap_or_default();
                        ("SCHEMA".to_owned(), name)
                    }
                    ast::GrantObjects::Sequences(names) => {
                        let name = names.first().map(std::string::ToString::to_string).unwrap_or_default();
                        ("SEQUENCE".to_owned(), name)
                    }
                    ast::GrantObjects::AllTablesInSchema { schemas } => {
                        let name = schemas.first().map(std::string::ToString::to_string).unwrap_or_default();
                        ("SCHEMA".to_owned(), name)
                    }
                    _ => {
                        return Err(SqlError::Unsupported("REVOKE on this object type".into()));
                    }
                };
                let grantee = grantees
                    .first()
                    .map(|g| g.value.clone())
                    .unwrap_or_default();
                Ok(BoundStatement::Revoke {
                    privilege: priv_str,
                    object_type: obj_type,
                    object_name: obj_name,
                    grantee,
                })
            }
            Statement::CreateSchema { schema_name, if_not_exists, .. } => {
                let name = schema_name.to_string();
                Ok(BoundStatement::CreateSchema {
                    name,
                    if_not_exists: *if_not_exists,
                })
            }
            Statement::DropFunction { if_exists, func_desc, .. } => {
                let desc = func_desc
                    .first()
                    .ok_or_else(|| SqlError::Parse("DROP FUNCTION requires a name".into()))?;
                Ok(BoundStatement::DropFunction {
                    name: desc.name.to_string(),
                    if_exists: *if_exists,
                })
            }
            Statement::CreateFunction {
                or_replace,
                name,
                args,
                return_type,
                function_body,
                language,
                behavior,
                called_on_null,
                ..
            } => self.bind_create_function(
                *or_replace, name, args, return_type, function_body, language, behavior, called_on_null,
            ),
            _ => Err(SqlError::Unsupported(format!(
                "Statement type: {:?}",
                std::mem::discriminant(stmt)
            ))),
        }
    }

    fn bind_copy(
        &self,
        source: &ast::CopySource,
        to: bool,
        target: &ast::CopyTarget,
        options: &[ast::CopyOption],
        legacy_options: &[ast::CopyLegacyOption],
    ) -> Result<BoundStatement, SqlError> {
        // Resolve target: STDIN/STDOUT or server-side FILE
        let file_path = match target {
            ast::CopyTarget::Stdin if to => {
                return Err(SqlError::Unsupported("COPY TO STDIN".into()));
            }
            ast::CopyTarget::Stdout if !to => {
                return Err(SqlError::Unsupported("COPY FROM STDOUT".into()));
            }
            ast::CopyTarget::Stdin | ast::CopyTarget::Stdout => None,
            ast::CopyTarget::File { filename } => Some(filename.clone()),
            ast::CopyTarget::Program { command } => {
                return Err(SqlError::Unsupported(format!("COPY PROGRAM '{command}'")));
            }
        };

        // Handle COPY (query) TO STDOUT
        if let ast::CopySource::Query(query) = source {
            if !to {
                return Err(SqlError::Unsupported(
                    "COPY (query) FROM is not supported".into(),
                ));
            }
            let bound_query = self.bind_select_query(query)?;

            // Parse format options for query COPY
            let mut csv = false;
            let mut delimiter = '\t';
            let mut header = false;
            let mut null_string = "\\N".to_owned();
            let mut quote = '"';
            let mut escape = '"';
            for opt in options {
                match opt {
                    ast::CopyOption::Format(ident) => {
                        let fmt = ident.value.to_lowercase();
                        match fmt.as_str() {
                            "csv" => {
                                csv = true;
                                delimiter = ',';
                            }
                            "text" => {
                                csv = false;
                                delimiter = '\t';
                            }
                            _ => return Err(SqlError::Unsupported(format!("COPY FORMAT {fmt}"))),
                        }
                    }
                    ast::CopyOption::Delimiter(c) => delimiter = *c,
                    ast::CopyOption::Header(b) => header = *b,
                    ast::CopyOption::Null(s) => null_string = s.clone(),
                    ast::CopyOption::Quote(c) => quote = *c,
                    ast::CopyOption::Escape(c) => escape = *c,
                    _ => {}
                }
            }
            return Ok(BoundStatement::CopyQueryTo {
                query: Box::new(bound_query),
                csv,
                delimiter,
                header,
                null_string,
                quote,
                escape,
                file_path: file_path.clone(),
            });
        }

        // Resolve table and columns
        let (table_name, col_idents) = match source {
            ast::CopySource::Table {
                table_name,
                columns,
            } => (table_name.to_string(), columns.clone()),
            ast::CopySource::Query(_) => unreachable!(),
        };

        let schema = self
            .catalog
            .find_table(&table_name)
            .ok_or_else(|| SqlError::UnknownTable(table_name.clone()))?;
        let table_id = schema.id;

        // Resolve columns (empty = all columns)
        let columns: Vec<usize> = if col_idents.is_empty() {
            (0..schema.columns.len()).collect()
        } else {
            col_idents
                .iter()
                .map(|ident| {
                    let col_name = ident.value.to_lowercase();
                    schema
                        .find_column(&col_name)
                        .ok_or(SqlError::UnknownColumn(col_name))
                })
                .collect::<Result<Vec<_>, _>>()?
        };

        // Parse format options
        let mut csv = false;
        let mut delimiter = '\t';
        let mut header = false;
        let mut null_string = "\\N".to_owned();
        let mut quote = '"';
        let mut escape = '"';

        for opt in options {
            match opt {
                ast::CopyOption::Format(ident) => {
                    let fmt = ident.value.to_lowercase();
                    match fmt.as_str() {
                        "csv" => {
                            csv = true;
                            delimiter = ',';
                        }
                        "text" => {
                            csv = false;
                            delimiter = '\t';
                        }
                        _ => return Err(SqlError::Unsupported(format!("COPY FORMAT {fmt}"))),
                    }
                }
                ast::CopyOption::Delimiter(c) => delimiter = *c,
                ast::CopyOption::Header(b) => header = *b,
                ast::CopyOption::Null(s) => null_string = s.clone(),
                ast::CopyOption::Quote(c) => quote = *c,
                ast::CopyOption::Escape(c) => escape = *c,
                _ => {} // ignore unsupported options
            }
        }

        // Handle legacy options (pre-9.0 syntax)
        for opt in legacy_options {
            match opt {
                ast::CopyLegacyOption::Delimiter(c) => delimiter = *c,
                ast::CopyLegacyOption::Null(s) => null_string = s.clone(),
                ast::CopyLegacyOption::Csv(csv_opts) => {
                    csv = true;
                    if delimiter == '\t' {
                        delimiter = ',';
                    }
                    for csv_opt in csv_opts {
                        match csv_opt {
                            ast::CopyLegacyCsvOption::Header => header = true,
                            ast::CopyLegacyCsvOption::Quote(c) => quote = *c,
                            ast::CopyLegacyCsvOption::Escape(c) => escape = *c,
                            _ => {}
                        }
                    }
                }
                _ => {}
            }
        }

        if to {
            Ok(BoundStatement::CopyTo {
                table_id,
                schema: schema.clone(),
                columns,
                csv,
                delimiter,
                header,
                null_string,
                quote,
                escape,
                file_path,
            })
        } else {
            Ok(BoundStatement::CopyFrom {
                table_id,
                schema: schema.clone(),
                columns,
                csv,
                delimiter,
                header,
                null_string,
                quote,
                escape,
                file_path,
            })
        }
    }

    fn bind_create_table(&mut self, create: &ast::CreateTable) -> Result<BoundStatement, SqlError> {
        let table_name = create.name.to_string();
        let mut columns = Vec::new();
        let mut pk_columns = Vec::new();
        let mut dynamic_defaults = std::collections::HashMap::new();

        for (i, col_def) in create.columns.iter().enumerate() {
            // Check for SERIAL type (parsed as Custom by sqlparser)
            let (data_type, is_serial) = match &col_def.data_type {
                ast::DataType::Custom(name, _) if name.to_string().to_lowercase() == "serial" => {
                    (DataType::Int32, true)
                }
                ast::DataType::Custom(name, _)
                    if name.to_string().to_lowercase() == "bigserial" =>
                {
                    (DataType::Int64, true)
                }
                other => (self.resolve_data_type(other)?, false),
            };
            let mut is_pk = false;
            let mut nullable = !is_serial;
            let mut default_value = None;
            let mut default_fn = None;

            for option in &col_def.options {
                match &option.option {
                    ast::ColumnOption::Unique { is_primary, .. } if *is_primary => {
                        is_pk = true;
                        nullable = false;
                    }
                    ast::ColumnOption::NotNull => {
                        nullable = false;
                    }
                    ast::ColumnOption::Null => {
                        nullable = true;
                    }
                    ast::ColumnOption::Default(expr) => match expr {
                        Expr::Value(ref val) => {
                            if let Ok(d) = self.value_to_datum(val) {
                                default_value = Some(d);
                            }
                        }
                        Expr::Function(f) => {
                            let fname = f.name.to_string().to_uppercase();
                            match fname.as_str() {
                                "NOW" | "CURRENT_TIMESTAMP" => {
                                    default_fn = Some(falcon_common::schema::DefaultFn::CurrentTimestamp);
                                }
                                "CURRENT_DATE" => {
                                    default_fn = Some(falcon_common::schema::DefaultFn::CurrentDate);
                                }
                                "CURRENT_TIME" => {
                                    default_fn = Some(falcon_common::schema::DefaultFn::CurrentTime);
                                }
                                "NEXTVAL" => {
                                    if let Some(seq_name) = Self::extract_nextval_seq_name(f) {
                                        default_fn = Some(falcon_common::schema::DefaultFn::Nextval(seq_name));
                                    }
                                }
                                _ => {}
                            }
                        }
                        Expr::Identifier(ident) => {
                            match ident.value.to_uppercase().as_str() {
                                "CURRENT_TIMESTAMP" | "NOW" => {
                                    default_fn = Some(falcon_common::schema::DefaultFn::CurrentTimestamp);
                                }
                                "CURRENT_DATE" => {
                                    default_fn = Some(falcon_common::schema::DefaultFn::CurrentDate);
                                }
                                "CURRENT_TIME" => {
                                    default_fn = Some(falcon_common::schema::DefaultFn::CurrentTime);
                                }
                                _ => {}
                            }
                        }
                        _ => {}
                    },
                    _ => {}
                }
            }

            if is_pk {
                pk_columns.push(i);
            }

            if let Some(dfn) = default_fn {
                dynamic_defaults.insert(i, dfn);
            }
            let max_length = Self::extract_char_length(&col_def.data_type);
            columns.push(ColumnDef {
                id: ColumnId(i as u32),
                name: col_def.name.value.clone(),
                data_type,
                nullable,
                is_primary_key: is_pk,
                default_value,
                is_serial,
                max_length,
            });
        }

        // Check table-level constraints for PRIMARY KEY, CHECK, and UNIQUE
        let mut check_constraints = Vec::new();
        let mut unique_constraints: Vec<Vec<usize>> = Vec::new();
        for constraint in &create.constraints {
            match constraint {
                ast::TableConstraint::PrimaryKey {
                    columns: pk_cols, ..
                } => {
                    for pk_col in pk_cols {
                        let col_name = pk_col.value.to_lowercase();
                        if let Some(idx) = columns
                            .iter()
                            .position(|c| c.name.to_lowercase() == col_name)
                        {
                            if !pk_columns.contains(&idx) {
                                pk_columns.push(idx);
                                columns[idx].is_primary_key = true;
                                columns[idx].nullable = false;
                            }
                        } else {
                            return Err(SqlError::UnknownColumn(pk_col.value.clone()));
                        }
                    }
                }
                ast::TableConstraint::Check { expr, .. } => {
                    check_constraints.push(format!("{expr}"));
                }
                ast::TableConstraint::Unique {
                    columns: uniq_cols, ..
                } => {
                    let mut indices = Vec::new();
                    for uc in uniq_cols {
                        let col_name = uc.value.to_lowercase();
                        if let Some(idx) = columns
                            .iter()
                            .position(|c| c.name.to_lowercase() == col_name)
                        {
                            indices.push(idx);
                        } else {
                            return Err(SqlError::UnknownColumn(uc.value.clone()));
                        }
                    }
                    unique_constraints.push(indices);
                }
                _ => {}
            }
        }

        // Also collect column-level CHECK, UNIQUE, and FOREIGN KEY constraints
        let mut foreign_keys = Vec::new();
        for (i, col_def) in create.columns.iter().enumerate() {
            for option in &col_def.options {
                match &option.option {
                    ast::ColumnOption::Check(expr) => {
                        check_constraints.push(format!("{expr}"));
                    }
                    ast::ColumnOption::Unique { is_primary, .. } if !*is_primary => {
                        unique_constraints.push(vec![i]);
                    }
                    ast::ColumnOption::ForeignKey {
                        foreign_table,
                        referred_columns,
                        on_delete,
                        on_update,
                        ..
                    } => {
                        let ref_cols: Vec<String> =
                            referred_columns.iter().map(|c| c.value.clone()).collect();
                        foreign_keys.push(falcon_common::schema::ForeignKey {
                            columns: vec![i],
                            ref_table: foreign_table.to_string(),
                            ref_columns: ref_cols,
                            on_delete: Self::resolve_fk_action(on_delete),
                            on_update: Self::resolve_fk_action(on_update),
                        });
                    }
                    _ => {}
                }
            }
        }

        // Also collect table-level FOREIGN KEY constraints
        for constraint in &create.constraints {
            if let ast::TableConstraint::ForeignKey {
                columns: fk_cols,
                foreign_table,
                referred_columns,
                on_delete,
                on_update,
                ..
            } = constraint
            {
                let mut local_indices = Vec::new();
                for fk_col in fk_cols {
                    let col_name = fk_col.value.to_lowercase();
                    if let Some(idx) = columns
                        .iter()
                        .position(|c| c.name.to_lowercase() == col_name)
                    {
                        local_indices.push(idx);
                    } else {
                        return Err(SqlError::UnknownColumn(fk_col.value.clone()));
                    }
                }
                let ref_cols: Vec<String> =
                    referred_columns.iter().map(|c| c.value.clone()).collect();
                foreign_keys.push(falcon_common::schema::ForeignKey {
                    columns: local_indices,
                    ref_table: foreign_table.to_string(),
                    ref_columns: ref_cols,
                    on_delete: Self::resolve_fk_action(on_delete),
                    on_update: Self::resolve_fk_action(on_update),
                });
            }
        }

        let table_id = TableId(self.next_table_id);
        self.next_table_id += 1;

        let mut next_serial_values = std::collections::HashMap::new();
        for (i, col) in columns.iter().enumerate() {
            if col.is_serial {
                next_serial_values.insert(i, 1i64);
            }
        }
        // Resolve storage engine type from ENGINE= option (like SingleStore)
        let storage_type = create.engine.as_ref().map_or(
            Self::default_storage_type(),
            |engine| match engine.name.to_lowercase().as_str() {
                "columnstore" => falcon_common::schema::StorageType::Columnstore,
                "disk" | "disk_rowstore" | "diskrowstore" => {
                    falcon_common::schema::StorageType::DiskRowstore
                }
                "lsm" | "lsm_rowstore" | "lsmrowstore" => {
                    falcon_common::schema::StorageType::LsmRowstore
                }
                "rocksdb" | "rocks" => {
                    falcon_common::schema::StorageType::RocksDbRowstore
                }
                "redb" => falcon_common::schema::StorageType::RedbRowstore,
                _ => Self::default_storage_type(),
            },
        );

        // Parse SHARD KEY and SHARDING policy from WITH options
        // Syntax: CREATE TABLE t (...) WITH (shard_key = 'col1,col2', sharding = 'hash')
        //   or:   CREATE TABLE t (...) WITH (sharding = 'reference')
        let mut shard_key_cols: Vec<usize> = Vec::new();
        let mut sharding_policy = falcon_common::schema::ShardingPolicy::None;

        for opt in &create.with_options {
            let key = opt.name.value.to_lowercase();
            match key.as_str() {
                "shard_key" => {
                    let val: String = match &opt.value {
                        Expr::Value(Value::SingleQuotedString(s))
                        | Expr::Value(Value::DoubleQuotedString(s)) => s.clone(),
                        other => format!("{other}"),
                    };
                    for part in val.split(',') {
                        let col_name = part.trim().to_lowercase();
                        if col_name.is_empty() {
                            continue;
                        }
                        if let Some(idx) = columns
                            .iter()
                            .position(|c| c.name.to_lowercase() == col_name)
                        {
                            if !shard_key_cols.contains(&idx) {
                                shard_key_cols.push(idx);
                            }
                        } else {
                            return Err(SqlError::UnknownColumn(col_name));
                        }
                    }
                    if sharding_policy == falcon_common::schema::ShardingPolicy::None {
                        sharding_policy = falcon_common::schema::ShardingPolicy::Hash;
                    }
                }
                "sharding" => {
                    let val: String = match &opt.value {
                        Expr::Value(Value::SingleQuotedString(s))
                        | Expr::Value(Value::DoubleQuotedString(s)) => s.to_lowercase(),
                        other => format!("{other}").to_lowercase(),
                    };
                    sharding_policy = match val.as_str() {
                        "hash" => falcon_common::schema::ShardingPolicy::Hash,
                        "reference" | "replicated" => {
                            falcon_common::schema::ShardingPolicy::Reference
                        }
                        _ => falcon_common::schema::ShardingPolicy::None,
                    };
                }
                _ => {}
            }
        }

        // If sharding=hash but no explicit shard_key, default to PK
        if sharding_policy == falcon_common::schema::ShardingPolicy::Hash
            && shard_key_cols.is_empty()
        {
            shard_key_cols = pk_columns.clone();
        }

        let schema = TableSchema {
            id: table_id,
            name: table_name,
            columns,
            primary_key_columns: pk_columns,
            next_serial_values,
            check_constraints,
            unique_constraints,
            foreign_keys,
            storage_type,
            shard_key: shard_key_cols,
            sharding_policy,
            range_bounds: Vec::new(),
            dynamic_defaults,
        };

        Ok(BoundStatement::CreateTable(BoundCreateTable {
            schema,
            if_not_exists: create.if_not_exists,
        }))
    }

    fn bind_alter_table(
        &self,
        name: &ast::ObjectName,
        operations: &[ast::AlterTableOperation],
    ) -> Result<BoundStatement, SqlError> {
        let table_name = name.to_string();
        // Verify table exists
        let _schema = self
            .catalog
            .find_table(&table_name)
            .ok_or_else(|| SqlError::UnknownTable(table_name.clone()))?;

        if operations.is_empty() {
            return Err(SqlError::Parse(
                "ALTER TABLE requires at least one operation".into(),
            ));
        }

        let mut ops = Vec::new();
        for operation in operations {
            let op = match operation {
                ast::AlterTableOperation::AddColumn { column_def, .. } => {
                    let data_type = self.resolve_data_type(&column_def.data_type)?;
                    let nullable = !column_def
                        .options
                        .iter()
                        .any(|o| matches!(o.option, ast::ColumnOption::NotNull));
                    AlterTableOp::AddColumn(falcon_common::schema::ColumnDef {
                        id: ColumnId(0), // will be assigned by executor
                        name: column_def.name.value.clone(),
                        data_type,
                        nullable,
                        is_primary_key: false,
                        default_value: None,
                        is_serial: false, max_length: None,
                    })
                }
                ast::AlterTableOperation::DropColumn { column_name, .. } => {
                    AlterTableOp::DropColumn(column_name.value.clone())
                }
                ast::AlterTableOperation::RenameColumn {
                    old_column_name,
                    new_column_name,
                } => AlterTableOp::RenameColumn {
                    old_name: old_column_name.value.clone(),
                    new_name: new_column_name.value.clone(),
                },
                ast::AlterTableOperation::RenameTable {
                    table_name: new_name,
                } => AlterTableOp::RenameTable {
                    new_name: new_name.to_string(),
                },
                ast::AlterTableOperation::AlterColumn { column_name, op } => match op {
                    ast::AlterColumnOperation::SetDataType { data_type, .. } => {
                        let new_type = self.resolve_data_type(data_type)?;
                        AlterTableOp::AlterColumnType {
                            column_name: column_name.value.clone(),
                            new_type,
                        }
                    }
                    ast::AlterColumnOperation::SetNotNull => AlterTableOp::AlterColumnSetNotNull {
                        column_name: column_name.value.clone(),
                    },
                    ast::AlterColumnOperation::DropNotNull => {
                        AlterTableOp::AlterColumnDropNotNull {
                            column_name: column_name.value.clone(),
                        }
                    }
                    ast::AlterColumnOperation::SetDefault { value } => {
                        let schema = _schema.clone();
                        let aliases = std::collections::HashMap::new();
                        let bound = self.bind_expr_with_aliases(value, &schema, &aliases)?;
                        AlterTableOp::AlterColumnSetDefault {
                            column_name: column_name.value.clone(),
                            default_expr: bound,
                        }
                    }
                    ast::AlterColumnOperation::DropDefault => {
                        AlterTableOp::AlterColumnDropDefault {
                            column_name: column_name.value.clone(),
                        }
                    }
                    other => {
                        return Err(SqlError::Unsupported(format!(
                            "ALTER COLUMN operation: {:?}",
                            std::mem::discriminant(other)
                        )))
                    }
                },
                other => {
                    return Err(SqlError::Unsupported(format!(
                        "ALTER TABLE operation: {:?}",
                        std::mem::discriminant(other)
                    )))
                }
            };
            ops.push(op);
        }

        Ok(BoundStatement::AlterTable(BoundAlterTable {
            table_name,
            ops,
        }))
    }

    fn bind_insert(&self, insert: &ast::Insert) -> Result<BoundStatement, SqlError> {
        let table_name = insert.table_name.to_string();
        let schema = self
            .catalog
            .find_table(&table_name)
            .ok_or_else(|| SqlError::UnknownTable(table_name.clone()))?
            .clone();

        // Resolve target columns
        let columns: Vec<usize> = if insert.columns.is_empty() {
            (0..schema.columns.len()).collect()
        } else {
            insert
                .columns
                .iter()
                .map(|c| {
                    schema
                        .find_column(&c.value)
                        .ok_or_else(|| SqlError::UnknownColumn(c.value.clone()))
                })
                .collect::<Result<Vec<_>, _>>()?
        };

        // Resolve value rows or SELECT source
        let source = insert
            .source
            .as_ref()
            .ok_or_else(|| SqlError::Parse("INSERT requires VALUES or SELECT".into()))?;
        match source.body.as_ref() {
            SetExpr::Values(values) => {
                let mut bound_rows = Vec::new();
                for row in &values.rows {
                    if row.len() != columns.len() {
                        return Err(SqlError::Parse(format!(
                            "INSERT has {} columns but {} values",
                            columns.len(),
                            row.len()
                        )));
                    }
                    let bound_exprs: Vec<BoundExpr> = row
                        .iter()
                        .map(|expr| self.bind_expr(expr, &schema))
                        .collect::<Result<Vec<_>, _>>()?;
                    bound_rows.push(bound_exprs);
                }
                let returning = self.bind_returning_items(&insert.returning, &schema)?;
                let on_conflict = self.bind_on_conflict(&insert.on, &schema)?;
                Ok(BoundStatement::Insert(BoundInsert {
                    table_id: schema.id,
                    table_name,
                    schema,
                    columns,
                    rows: bound_rows,
                    source_select: None,
                    returning,
                    on_conflict,
                }))
            }
            SetExpr::Select(_) => {
                // INSERT ... SELECT — bind the source query
                let bound_select = self.bind_select_query(source)?;
                let returning = self.bind_returning_items(&insert.returning, &schema)?;
                let on_conflict = self.bind_on_conflict(&insert.on, &schema)?;
                Ok(BoundStatement::Insert(BoundInsert {
                    table_id: schema.id,
                    table_name,
                    schema,
                    columns,
                    rows: Vec::new(),
                    source_select: Some(bound_select),
                    returning,
                    on_conflict,
                }))
            }
            _ => Err(SqlError::Unsupported(
                "INSERT source must be VALUES or SELECT".into(),
            )),
        }
    }

    fn bind_update(
        &self,
        table: &ast::TableWithJoins,
        assignments: &[ast::Assignment],
        selection: &Option<Expr>,
        returning_items: &Option<Vec<ast::SelectItem>>,
        from: &Option<ast::TableWithJoins>,
    ) -> Result<BoundStatement, SqlError> {
        let table_name = table.relation.to_string();
        let schema = self
            .catalog
            .find_table(&table_name)
            .ok_or_else(|| SqlError::UnknownTable(table_name.clone()))?
            .clone();

        // Build combined schema if FROM clause is present
        let (combined_schema, from_table) = if let Some(from_twj) = from {
            let from_name = from_twj.relation.to_string();
            let from_schema = self
                .catalog
                .find_table(&from_name)
                .ok_or_else(|| SqlError::UnknownTable(from_name.clone()))?
                .clone();
            let col_offset = schema.columns.len();
            let mut combined_cols = schema.columns.clone();
            combined_cols.extend(from_schema.columns.clone());
            let combined = TableSchema {
                id: schema.id,
                name: schema.name.clone(),
                columns: combined_cols,
                primary_key_columns: schema.primary_key_columns.clone(),
                next_serial_values: schema.next_serial_values.clone(),
                check_constraints: schema.check_constraints.clone(),
                unique_constraints: schema.unique_constraints.clone(),
                foreign_keys: schema.foreign_keys.clone(),
                ..Default::default()
            };
            let bound_from = BoundFromTable {
                table_id: from_schema.id,
                table_name: from_name,
                schema: from_schema,
                col_offset,
            };
            (combined, Some(bound_from))
        } else {
            (schema.clone(), None)
        };

        // Build alias map for combined schema
        let mut aliases: AliasMap = std::collections::HashMap::new();
        aliases.insert(table_name.to_lowercase(), (table_name.clone(), 0));
        if let Some(ref ft) = from_table {
            aliases.insert(
                ft.table_name.to_lowercase(),
                (ft.table_name.clone(), ft.col_offset),
            );
        }

        let bound_assignments: Vec<(usize, BoundExpr)> = assignments
            .iter()
            .map(|a| {
                let col_name = match &a.target {
                    ast::AssignmentTarget::ColumnName(name) => name.to_string(),
                    ast::AssignmentTarget::Tuple(names) => names
                        .iter()
                        .map(std::string::ToString::to_string)
                        .collect::<Vec<_>>()
                        .join("."),
                };
                let col_idx = schema
                    .find_column(&col_name)
                    .ok_or(SqlError::UnknownColumn(col_name))?;
                let expr = self.bind_expr_with_aliases(&a.value, &combined_schema, &aliases)?;
                Ok((col_idx, expr))
            })
            .collect::<Result<Vec<_>, SqlError>>()?;

        let filter = selection
            .as_ref()
            .map(|expr| self.bind_expr_with_aliases(expr, &combined_schema, &aliases))
            .transpose()?;

        let returning = self.bind_returning_items(returning_items, &schema)?;
        Ok(BoundStatement::Update(BoundUpdate {
            table_id: schema.id,
            table_name,
            schema,
            assignments: bound_assignments,
            filter,
            returning,
            from_table,
        }))
    }

    fn bind_delete(&self, delete: &ast::Delete) -> Result<BoundStatement, SqlError> {
        let from_tables = match &delete.from {
            ast::FromTable::WithFromKeyword(tables)
            | ast::FromTable::WithoutKeyword(tables) => tables,
        };
        let from = from_tables
            .first()
            .ok_or_else(|| SqlError::Parse("DELETE requires FROM".into()))?;
        let table_name = from.relation.to_string();
        let schema = self
            .catalog
            .find_table(&table_name)
            .ok_or_else(|| SqlError::UnknownTable(table_name.clone()))?
            .clone();

        // Handle USING clause for multi-table DELETE
        let (combined_schema, using_table) = if let Some(using_tables) = &delete.using {
            if let Some(using_twj) = using_tables.first() {
                let using_name = using_twj.relation.to_string();
                let using_schema = self
                    .catalog
                    .find_table(&using_name)
                    .ok_or_else(|| SqlError::UnknownTable(using_name.clone()))?
                    .clone();
                let col_offset = schema.columns.len();
                let mut combined_cols = schema.columns.clone();
                combined_cols.extend(using_schema.columns.clone());
                let combined = TableSchema {
                    id: schema.id,
                    name: schema.name.clone(),
                    columns: combined_cols,
                    primary_key_columns: schema.primary_key_columns.clone(),
                    next_serial_values: schema.next_serial_values.clone(),
                    check_constraints: schema.check_constraints.clone(),
                    unique_constraints: schema.unique_constraints.clone(),
                    foreign_keys: schema.foreign_keys.clone(),
                    ..Default::default()
                };
                let bound_using = BoundFromTable {
                    table_id: using_schema.id,
                    table_name: using_name,
                    schema: using_schema,
                    col_offset,
                };
                (combined, Some(bound_using))
            } else {
                (schema.clone(), None)
            }
        } else {
            (schema.clone(), None)
        };

        // Build alias map for combined schema
        let mut aliases: AliasMap = std::collections::HashMap::new();
        aliases.insert(table_name.to_lowercase(), (table_name.clone(), 0));
        if let Some(ref ut) = using_table {
            aliases.insert(
                ut.table_name.to_lowercase(),
                (ut.table_name.clone(), ut.col_offset),
            );
        }

        let filter = delete
            .selection
            .as_ref()
            .map(|expr| self.bind_expr_with_aliases(expr, &combined_schema, &aliases))
            .transpose()?;

        let returning = self.bind_returning_items(&delete.returning, &schema)?;
        Ok(BoundStatement::Delete(BoundDelete {
            table_id: schema.id,
            table_name,
            schema,
            filter,
            returning,
            using_table,
        }))
    }

    pub(crate) fn extract_table_name(
        &self,
        relation: &ast::TableFactor,
    ) -> Result<(String, Option<String>), SqlError> {
        match relation {
            ast::TableFactor::Table { name, alias, .. } => {
                let table_name = name.to_string();
                let alias_name = alias.as_ref().map(|a| a.name.value.clone());
                Ok((table_name, alias_name))
            }
            ast::TableFactor::Derived { alias, .. } => {
                // Derived table (subquery) — use alias as table name
                let alias_name = alias
                    .as_ref()
                    .map(|a| a.name.value.clone())
                    .unwrap_or_else(|| "__derived__".to_owned());
                Ok((alias_name.clone(), Some(alias_name)))
            }
            _ => Err(SqlError::Unsupported("Non-table FROM source".into())),
        }
    }

    fn extract_nextval_seq_name(f: &ast::Function) -> Option<String> {
        if let ast::FunctionArguments::List(args) = &f.args {
            if let Some(ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(expr))) = args.args.first() {
                match expr {
                    Expr::Value(Value::SingleQuotedString(s)) => return Some(s.clone()),
                    Expr::Value(Value::DoubleQuotedString(s)) => return Some(s.clone()),
                    _ => {}
                }
            }
        }
        None
    }

    pub(crate) fn value_to_datum(&self, value: &Value) -> Result<Datum, SqlError> {
        match value {
            Value::Number(n, _) => {
                if let Ok(i) = n.parse::<i32>() {
                    Ok(Datum::Int32(i))
                } else if let Ok(i) = n.parse::<i64>() {
                    Ok(Datum::Int64(i))
                } else if let Ok(f) = n.parse::<f64>() {
                    Ok(Datum::Float64(f))
                } else {
                    Err(SqlError::Parse(format!("Cannot parse number: {n}")))
                }
            }
            Value::SingleQuotedString(s) | Value::DoubleQuotedString(s) => {
                Ok(Datum::Text(s.clone()))
            }
            Value::Boolean(b) => Ok(Datum::Boolean(*b)),
            Value::Null => Ok(Datum::Null),
            _ => Err(SqlError::Unsupported(format!("Value type: {value:?}"))),
        }
    }

    /// Evaluate a constant expression (for GENERATE_SERIES args etc.) — always returns Int64 for integers.
    pub(crate) fn eval_const_expr(&self, expr: &Expr) -> Result<Datum, SqlError> {
        match expr {
            Expr::Value(v) => {
                let d = self.value_to_datum(v)?;
                // Promote Int32 to Int64 for consistency
                match d {
                    Datum::Int32(n) => Ok(Datum::Int64(i64::from(n))),
                    other => Ok(other),
                }
            }
            Expr::UnaryOp {
                op: ast::UnaryOperator::Minus,
                expr: inner,
            } => {
                let val = self.eval_const_expr(inner)?;
                match val {
                    Datum::Int64(n) => Ok(Datum::Int64(-n)),
                    Datum::Float64(f) => Ok(Datum::Float64(-f)),
                    _ => Err(SqlError::Parse("Cannot negate non-numeric".into())),
                }
            }
            Expr::Array(ast::Array { elem, .. }) => {
                let elems: Vec<Datum> = elem
                    .iter()
                    .map(|e| self.eval_const_expr(e))
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(Datum::Array(elems))
            }
            _ => Err(SqlError::Parse("Expected constant expression".into())),
        }
    }

    pub(crate) fn resolve_bin_op(&self, op: &ast::BinaryOperator) -> Result<BinOp, SqlError> {
        match op {
            ast::BinaryOperator::Eq => Ok(BinOp::Eq),
            ast::BinaryOperator::NotEq => Ok(BinOp::NotEq),
            ast::BinaryOperator::Lt => Ok(BinOp::Lt),
            ast::BinaryOperator::LtEq => Ok(BinOp::LtEq),
            ast::BinaryOperator::Gt => Ok(BinOp::Gt),
            ast::BinaryOperator::GtEq => Ok(BinOp::GtEq),
            ast::BinaryOperator::And => Ok(BinOp::And),
            ast::BinaryOperator::Or => Ok(BinOp::Or),
            ast::BinaryOperator::Plus => Ok(BinOp::Plus),
            ast::BinaryOperator::Minus => Ok(BinOp::Minus),
            ast::BinaryOperator::Multiply => Ok(BinOp::Multiply),
            ast::BinaryOperator::Divide => Ok(BinOp::Divide),
            ast::BinaryOperator::Modulo => Ok(BinOp::Modulo),
            ast::BinaryOperator::StringConcat => Ok(BinOp::StringConcat),
            ast::BinaryOperator::Arrow => Ok(BinOp::JsonArrow),
            ast::BinaryOperator::LongArrow => Ok(BinOp::JsonArrowText),
            ast::BinaryOperator::HashArrow => Ok(BinOp::JsonHashArrow),
            ast::BinaryOperator::HashLongArrow => Ok(BinOp::JsonHashArrowText),
            ast::BinaryOperator::AtArrow => Ok(BinOp::JsonContains),
            ast::BinaryOperator::ArrowAt => Ok(BinOp::JsonContainedBy),
            ast::BinaryOperator::Question => Ok(BinOp::JsonExists),
            ast::BinaryOperator::AtAt => Ok(BinOp::TsMatch),
            _ => Err(SqlError::Unsupported(format!("Operator: {op:?}"))),
        }
    }

    const fn resolve_fk_action(
        action: &Option<ast::ReferentialAction>,
    ) -> falcon_common::schema::FkAction {
        match action {
            Some(ast::ReferentialAction::Cascade) => falcon_common::schema::FkAction::Cascade,
            Some(ast::ReferentialAction::SetNull) => falcon_common::schema::FkAction::SetNull,
            Some(ast::ReferentialAction::SetDefault) => falcon_common::schema::FkAction::SetDefault,
            Some(ast::ReferentialAction::Restrict) => falcon_common::schema::FkAction::Restrict,
            Some(ast::ReferentialAction::NoAction) | None => {
                falcon_common::schema::FkAction::NoAction
            }
        }
    }

    /// Extract character max length from VARCHAR(n)/CHAR(n) AST types.
    fn extract_char_length(dt: &ast::DataType) -> Option<u32> {
        match dt {
            ast::DataType::Varchar(Some(ast::CharacterLength::IntegerLength { length, .. }))
            | ast::DataType::CharVarying(Some(ast::CharacterLength::IntegerLength { length, .. }))
            | ast::DataType::Char(Some(ast::CharacterLength::IntegerLength { length, .. }))
            | ast::DataType::Character(Some(ast::CharacterLength::IntegerLength { length, .. })) => {
                Some(*length as u32)
            }
            _ => None,
        }
    }

    pub(crate) fn resolve_data_type(&self, dt: &ast::DataType) -> Result<DataType, SqlError> {
        match dt {
            ast::DataType::Boolean | ast::DataType::Bool => Ok(DataType::Boolean),
            ast::DataType::SmallInt(None)
            | ast::DataType::Int2(_)
            | ast::DataType::TinyInt(_) => Ok(DataType::Int16),
            ast::DataType::Int(None) | ast::DataType::Integer(None) | ast::DataType::Int4(_)
            | ast::DataType::Regclass => Ok(DataType::Int32),
            ast::DataType::BigInt(None) | ast::DataType::Int8(_)
            | ast::DataType::UnsignedTinyInt(_)
            | ast::DataType::UnsignedSmallInt(_)
            | ast::DataType::UnsignedInt(_)
            | ast::DataType::UnsignedInteger(_)
            | ast::DataType::UnsignedBigInt(_)
            | ast::DataType::UnsignedInt8(_) => Ok(DataType::Int64),
            ast::DataType::Real
            | ast::DataType::Float4 => Ok(DataType::Float32),
            ast::DataType::Float8
            | ast::DataType::Float(None)
            | ast::DataType::DoublePrecision
            | ast::DataType::Double => Ok(DataType::Float64),
            ast::DataType::Numeric(info) | ast::DataType::Decimal(info) => {
                let (p, s) = match info {
                    ast::ExactNumberInfo::PrecisionAndScale(p, s) => (*p as u8, *s as u8),
                    ast::ExactNumberInfo::Precision(p) => (*p as u8, 0),
                    ast::ExactNumberInfo::None => (38, 10),
                };
                Ok(DataType::Decimal(p, s))
            }
            ast::DataType::Text
            | ast::DataType::Varchar(_)
            | ast::DataType::CharVarying(_)
            | ast::DataType::String(_)
            | ast::DataType::Char(_)
            | ast::DataType::Character(_)
            | ast::DataType::Enum(_) => Ok(DataType::Text),
            ast::DataType::Interval => Ok(DataType::Interval),
            ast::DataType::Uuid => Ok(DataType::Uuid),
            ast::DataType::Bytea
            | ast::DataType::Blob(_)
            | ast::DataType::Binary(_)
            | ast::DataType::Varbinary(_) => Ok(DataType::Bytea),
            ast::DataType::Timestamp(_, _) => Ok(DataType::Timestamp),
            ast::DataType::Time(_, _) => Ok(DataType::Time),
            ast::DataType::Date => Ok(DataType::Date),
            ast::DataType::Array(inner) => {
                let elem_type = match inner {
                    ast::ArrayElemTypeDef::AngleBracket(ref dt)
                    | ast::ArrayElemTypeDef::SquareBracket(ref dt, _)
                    | ast::ArrayElemTypeDef::Parenthesis(ref dt) => self.resolve_data_type(dt)?,
                    ast::ArrayElemTypeDef::None => DataType::Text,
                };
                Ok(DataType::Array(Box::new(elem_type)))
            }
            ast::DataType::JSON | ast::DataType::JSONB => Ok(DataType::Jsonb),
            // Map additional PG types to closest FalconDB equivalents
            ast::DataType::Custom(name, _) => {
                let type_name = name.to_string().to_lowercase();
                match type_name.as_str() {
                    "inet" | "cidr" | "macaddr"
                    | "name" | "varchar" | "character varying" | "bpchar" => Ok(DataType::Text),
                    "uuid" => Ok(DataType::Uuid),
                    "bytea" => Ok(DataType::Bytea),
                    "interval" => Ok(DataType::Interval),
                    "time" => Ok(DataType::Time),
                    "date" => Ok(DataType::Date),
                    "int2" | "smallint" => Ok(DataType::Int16),
                    "oid" | "regclass" | "regtype"
                    | "int4" | "integer" | "int" => Ok(DataType::Int32),
                    "int8" | "bigint" => Ok(DataType::Int64),
                    "float4" | "real" => Ok(DataType::Float32),
                    "money" | "float8" | "double precision" => Ok(DataType::Float64),
                    "numeric" | "decimal" => Ok(DataType::Decimal(38, 10)),
                    "timestamp" | "timestamptz" => Ok(DataType::Timestamp),
                    "tsvector" => Ok(DataType::TsVector),
                    "tsquery" => Ok(DataType::TsQuery),
                    _ => Err(SqlError::Unsupported(format!("Data type: {type_name}"))),
                }
            }
            _ => Err(SqlError::Unsupported(format!("Data type: {dt:?}"))),
        }
    }

    fn bind_create_function(
        &self,
        or_replace: bool,
        name: &ast::ObjectName,
        args: &Option<Vec<ast::OperateFunctionArg>>,
        return_type: &Option<ast::DataType>,
        function_body: &Option<ast::CreateFunctionBody>,
        language: &Option<ast::Ident>,
        behavior: &Option<ast::FunctionBehavior>,
        called_on_null: &Option<ast::FunctionCalledOnNull>,
    ) -> Result<BoundStatement, SqlError> {
        use falcon_common::schema::{FunctionDef, FunctionLanguage, FunctionParam, FunctionVolatility};

        let func_name = name.to_string();

        // Parse parameters
        let params = if let Some(arg_list) = args {
            arg_list
                .iter()
                .map(|arg| {
                    let param_name = arg.name.as_ref().map(|n| n.value.clone());
                    let data_type = self.resolve_data_type(&arg.data_type)?;
                    Ok(FunctionParam { name: param_name, data_type })
                })
                .collect::<Result<Vec<_>, SqlError>>()?
        } else {
            Vec::new()
        };

        // Parse return type
        let ret_type = return_type
            .as_ref()
            .map(|dt| self.resolve_data_type(dt))
            .transpose()?;

        // Parse language
        let lang_str = language
            .as_ref()
            .map(|l| l.value.to_lowercase())
            .unwrap_or_else(|| "sql".to_owned());
        let lang = match lang_str.as_str() {
            "sql" => FunctionLanguage::Sql,
            "plpgsql" => FunctionLanguage::PlPgSql,
            other => return Err(SqlError::Unsupported(format!("LANGUAGE {other}"))),
        };

        // Extract function body
        let body = match function_body {
            Some(ast::CreateFunctionBody::AsBeforeOptions(expr))
            | Some(ast::CreateFunctionBody::AsAfterOptions(expr)) => {
                format!("{expr}")
            }
            Some(ast::CreateFunctionBody::Return(expr)) => {
                format!("RETURN {expr}")
            }
            None => return Err(SqlError::Parse("CREATE FUNCTION requires a body".into())),
        };
        // Strip surrounding single-quotes if present (dollar-quoted strings arrive as string literals)
        let body = body.trim_matches('\'').to_owned();

        let volatility = match behavior {
            Some(ast::FunctionBehavior::Immutable) => FunctionVolatility::Immutable,
            Some(ast::FunctionBehavior::Stable) => FunctionVolatility::Stable,
            Some(ast::FunctionBehavior::Volatile) | None => FunctionVolatility::Volatile,
        };

        let is_strict = matches!(
            called_on_null,
            Some(ast::FunctionCalledOnNull::Strict)
            | Some(ast::FunctionCalledOnNull::ReturnsNullOnNullInput)
        );

        Ok(BoundStatement::CreateFunction {
            def: FunctionDef {
                name: func_name,
                params,
                return_type: ret_type,
                language: lang,
                body,
                volatility,
                is_strict,
                or_replace,
            },
        })
    }

    /// Extract optional column index from aggregate/window function args.
    pub(crate) fn bind_agg_col_idx(
        &self,
        func: &ast::Function,
        schema: &TableSchema,
    ) -> Result<Option<usize>, SqlError> {
        match &func.args {
            ast::FunctionArguments::List(args) => {
                if args.args.is_empty() {
                    Ok(None)
                } else if let Some(ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(
                    Expr::Identifier(ident),
                ))) = args.args.first()
                {
                    Ok(Some(schema.find_column(&ident.value).ok_or_else(|| {
                        SqlError::UnknownColumn(ident.value.clone())
                    })?))
                } else if matches!(args.args.first(), Some(ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Wildcard))) {
                    Ok(None)
                } else {
                    // Non-column arg (e.g. integer literal for NTILE) — return None
                    Ok(None)
                }
            }
            ast::FunctionArguments::None => Ok(None),
            _ => Err(SqlError::Unsupported("Subquery in function".into())),
        }
    }

    /// Extract an integer literal from function args at the given position.
    pub(crate) fn extract_int_arg(
        &self,
        func: &ast::Function,
        pos: usize,
    ) -> Result<Option<i64>, SqlError> {
        match &func.args {
            ast::FunctionArguments::List(args) => {
                if let Some(ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(expr))) =
                    args.args.get(pos)
                {
                    match expr {
                        Expr::Value(Value::Number(n, _)) => {
                            Ok(Some(n.parse::<i64>().map_err(|_| {
                                SqlError::Unsupported("Invalid integer".into())
                            })?))
                        }
                        _ => Ok(None),
                    }
                } else {
                    Ok(None)
                }
            }
            _ => Ok(None),
        }
    }

    /// Bind RETURNING clause items to column indices.
    pub(crate) fn bind_returning_items(
        &self,
        returning: &Option<Vec<ast::SelectItem>>,
        schema: &TableSchema,
    ) -> Result<Vec<(BoundExpr, String)>, SqlError> {
        let items = match returning {
            Some(items) => items,
            None => return Ok(Vec::new()),
        };
        let mut result = Vec::new();
        for item in items {
            match item {
                ast::SelectItem::Wildcard(_) => {
                    for (idx, col) in schema.columns.iter().enumerate() {
                        result.push((BoundExpr::ColumnRef(idx), col.name.clone()));
                    }
                }
                ast::SelectItem::UnnamedExpr(expr) => {
                    let bound = self.bind_expr(expr, schema)?;
                    let alias = if let BoundExpr::ColumnRef(idx) = &bound {
                        schema.columns[*idx].name.clone()
                    } else {
                        format!("{expr}")
                    };
                    result.push((bound, alias));
                }
                ast::SelectItem::ExprWithAlias { expr, alias } => {
                    let bound = self.bind_expr(expr, schema)?;
                    result.push((bound, alias.value.clone()));
                }
                _ => return Err(SqlError::Unsupported("Complex RETURNING item".into())),
            }
        }
        Ok(result)
    }

    /// Expand a view into a CTE: parse its SQL, bind it, build a schema, and register
    /// it in cte_schemas/bound_ctes so the executor materializes the view data.
    /// Returns the derived TableSchema for the view.
    pub(crate) fn bind_view_as_cte(
        &self,
        view_name: &str,
        query_sql: &str,
        cte_schemas: &mut std::collections::HashMap<String, TableSchema>,
        bound_ctes: &mut Vec<crate::types::BoundCte>,
    ) -> Result<TableSchema, SqlError> {
        use crate::parse_sql;
        use crate::types::BoundProjection;

        let stmts = parse_sql(query_sql)?;
        if stmts.is_empty() {
            return Err(SqlError::Parse(format!(
                "View '{view_name}' has empty SQL"
            )));
        }

        let mut inner_binder = Self::new(self.catalog.clone());
        let bound = inner_binder.bind(&stmts[0])?;

        let view_select = match bound {
            BoundStatement::Select(sel) => sel,
            _ => {
                return Err(SqlError::Parse(format!(
                    "View '{view_name}' must be a SELECT"
                )))
            }
        };

        let view_table_id = TableId(2_000_000 + bound_ctes.len() as u64);

        let mut columns = Vec::new();
        let vis = view_select.visible_projection_count;
        for (i, proj) in view_select.projections.iter().take(vis).enumerate() {
            let (col_name, data_type) = match proj {
                BoundProjection::Column(idx, alias) => {
                    let col = &view_select.schema.columns[*idx];
                    let name = if alias.is_empty() {
                        col.name.clone()
                    } else {
                        alias.clone()
                    };
                    (name, col.data_type.clone())
                }
                BoundProjection::Aggregate(_, _, alias, _, _) => (alias.clone(), DataType::Float64),
                BoundProjection::Expr(_, alias) => (alias.clone(), DataType::Text),
                BoundProjection::Window(w) => (w.alias.clone(), DataType::Int64),
            };
            columns.push(ColumnDef {
                id: ColumnId(i as u32),
                name: col_name,
                data_type,
                nullable: true,
                is_primary_key: false,
                default_value: None,
                is_serial: false, max_length: None,
            });
        }

        let view_schema = TableSchema {
            id: view_table_id,
            name: view_name.to_owned(),
            columns,
            primary_key_columns: vec![],
            next_serial_values: std::collections::HashMap::new(),
            check_constraints: vec![],
            unique_constraints: vec![],
            foreign_keys: vec![],
            ..Default::default()
        };

        cte_schemas.insert(view_name.to_lowercase(), view_schema.clone());
        bound_ctes.push(crate::types::BoundCte {
            name: view_name.to_lowercase(),
            table_id: view_table_id,
            select: view_select,
            recursive_select: None,
        });

        Ok(view_schema)
    }

    /// Bind ON CONFLICT clause from INSERT statement.
    /// Supports `excluded.col` references: the expression schema is extended with
    /// a duplicate set of columns at offset `num_cols`, and `excluded` is registered
    /// as a table alias pointing to that offset.
    fn bind_merge(
        &self,
        table: &ast::TableFactor,
        source: &ast::TableFactor,
        on: &ast::Expr,
        clauses: &[ast::MergeClause],
    ) -> Result<BoundStatement, SqlError> {
        // Resolve target table
        let target_name = match table {
            ast::TableFactor::Table { name, .. } => name.to_string(),
            _ => return Err(SqlError::Unsupported("MERGE non-table target".into())),
        };
        let target_schema = self.catalog.find_table(&target_name)
            .ok_or_else(|| SqlError::UnknownTable(target_name.clone()))?
            .clone();

        // Resolve source table
        let (source_name, source_alias) = match source {
            ast::TableFactor::Table { name, alias, .. } => {
                let n = name.to_string();
                let a = alias.as_ref().map(|a| a.name.value.clone());
                (n, a)
            }
            _ => return Err(SqlError::Unsupported("MERGE non-table source".into())),
        };
        let source_schema = self.catalog.find_table(&source_name)
            .ok_or_else(|| SqlError::UnknownTable(source_name.clone()))?
            .clone();

        // Build combined schema: [target cols] ++ [source cols]
        let target_ncols = target_schema.columns.len();
        let mut combined_columns = target_schema.columns.clone();
        for col in &source_schema.columns {
            combined_columns.push(falcon_common::schema::ColumnDef {
                id: falcon_common::types::ColumnId(combined_columns.len() as u32),
                name: col.name.clone(),
                data_type: col.data_type.clone(),
                nullable: col.nullable,
                is_primary_key: false,
                default_value: col.default_value.clone(),
                is_serial: false,
                max_length: col.max_length,
            });
        }
        let combined_schema = TableSchema {
            id: target_schema.id,
            name: target_schema.name.clone(),
            columns: combined_columns,
            primary_key_columns: target_schema.primary_key_columns.clone(),
            next_serial_values: std::collections::HashMap::new(),
            check_constraints: vec![],
            unique_constraints: vec![],
            foreign_keys: vec![],
            ..Default::default()
        };

        // Register aliases so source.col / target.col resolve correctly
        let mut aliases: AliasMap = std::collections::HashMap::new();
        aliases.insert(target_name.to_lowercase(), (target_schema.name.clone(), 0));
        let src_key = source_alias.as_deref().unwrap_or(&source_name).to_lowercase();
        aliases.insert(src_key, (target_schema.name.clone(), target_ncols));

        // Bind ON expression
        let on_expr = self.bind_expr_with_aliases(on, &combined_schema, &aliases)?;

        // Bind clauses
        let mut bound_clauses = Vec::new();
        for clause in clauses {
            let kind = match clause.clause_kind {
                ast::MergeClauseKind::Matched => MergeClauseKind::Matched,
                ast::MergeClauseKind::NotMatched
                | ast::MergeClauseKind::NotMatchedByTarget => MergeClauseKind::NotMatched,
                ast::MergeClauseKind::NotMatchedBySource => {
                    return Err(SqlError::Unsupported("NOT MATCHED BY SOURCE".into()));
                }
            };
            let predicate = clause.predicate.as_ref()
                .map(|e| self.bind_expr_with_aliases(e, &combined_schema, &aliases))
                .transpose()?;
            let action = match &clause.action {
                ast::MergeAction::Delete => BoundMergeAction::Delete,
                ast::MergeAction::Update { assignments } => {
                    let mut bound_assignments = Vec::new();
                    for assign in assignments {
                        let col_name = match &assign.target {
                            ast::AssignmentTarget::ColumnName(name) => name.to_string(),
                            _ => return Err(SqlError::Unsupported("Tuple assignment".into())),
                        };
                        let col_idx = target_schema.find_column(&col_name)
                            .ok_or_else(|| SqlError::UnknownColumn(col_name.clone()))?;
                        let expr = self.bind_expr_with_aliases(
                            &assign.value, &combined_schema, &aliases,
                        )?;
                        bound_assignments.push((col_idx, expr));
                    }
                    BoundMergeAction::Update(bound_assignments)
                }
                ast::MergeAction::Insert(insert_expr) => {
                    let cols: Vec<usize> = if insert_expr.columns.is_empty() {
                        (0..target_schema.columns.len()).collect()
                    } else {
                        insert_expr.columns.iter().map(|ident| {
                            target_schema.find_column(&ident.value)
                                .ok_or_else(|| SqlError::UnknownColumn(ident.value.clone()))
                        }).collect::<Result<Vec<_>, _>>()?
                    };
                    let values = match &insert_expr.kind {
                        ast::MergeInsertKind::Values(vals) => {
                            let row = vals.rows.first()
                                .ok_or_else(|| SqlError::Parse("MERGE INSERT requires VALUES".into()))?;
                            row.iter()
                                .map(|e| self.bind_expr_with_aliases(e, &combined_schema, &aliases))
                                .collect::<Result<Vec<_>, _>>()?
                        }
                        _ => return Err(SqlError::Unsupported("MERGE INSERT ROW".into())),
                    };
                    BoundMergeAction::Insert(cols, values)
                }
            };
            bound_clauses.push(BoundMergeClause { kind, predicate, action });
        }

        Ok(BoundStatement::Merge(BoundMerge {
            target_table_id: target_schema.id,
            target_name: target_schema.name.clone(),
            target_schema: target_schema.clone(),
            source_table_id: source_schema.id,
            source_name: source_schema.name.clone(),
            source_schema: source_schema.clone(),
            on_expr,
            clauses: bound_clauses,
            source_col_offset: target_ncols,
        }))
    }

    pub(crate) fn bind_on_conflict(
        &self,
        on: &Option<ast::OnInsert>,
        schema: &TableSchema,
    ) -> Result<Option<OnConflictAction>, SqlError> {
        let on_insert = match on {
            Some(on) => on,
            None => return Ok(None),
        };
        match on_insert {
            ast::OnInsert::OnConflict(conflict) => {
                match &conflict.action {
                    ast::OnConflictAction::DoNothing => Ok(Some(OnConflictAction::DoNothing)),
                    ast::OnConflictAction::DoUpdate(do_update) => {
                        // Build extended schema: [existing cols] ++ [excluded cols]
                        // so excluded.col resolves to ColumnRef(num_cols + col_idx)
                        let num_cols = schema.columns.len();
                        let mut extended_columns = schema.columns.clone();
                        for col in &schema.columns {
                            extended_columns.push(falcon_common::schema::ColumnDef {
                                id: falcon_common::types::ColumnId(extended_columns.len() as u32),
                                name: col.name.clone(),
                                data_type: col.data_type.clone(),
                                nullable: col.nullable,
                                is_primary_key: false,
                                default_value: col.default_value.clone(),
                                is_serial: false, max_length: None,
                            });
                        }
                        let extended_schema = TableSchema {
                            id: schema.id,
                            name: schema.name.clone(),
                            columns: extended_columns,
                            primary_key_columns: schema.primary_key_columns.clone(),
                            next_serial_values: std::collections::HashMap::new(),
                            check_constraints: vec![],
                            unique_constraints: vec![],
                            foreign_keys: vec![],
                            ..Default::default()
                        };
                        let mut aliases: AliasMap = std::collections::HashMap::new();
                        aliases.insert("excluded".to_owned(), (schema.name.clone(), num_cols));

                        let mut assignments = Vec::new();
                        for assign in &do_update.assignments {
                            let col_name = match &assign.target {
                                ast::AssignmentTarget::ColumnName(name) => name.to_string(),
                                _ => {
                                    return Err(SqlError::Unsupported(
                                        "Tuple assignment target".into(),
                                    ))
                                }
                            };
                            let col_idx = schema
                                .find_column(&col_name)
                                .ok_or_else(|| SqlError::UnknownColumn(col_name.clone()))?;
                            let expr = self.bind_expr_with_aliases(
                                &assign.value,
                                &extended_schema,
                                &aliases,
                            )?;
                            assignments.push((col_idx, expr));
                        }
                        let where_clause = do_update.selection.as_ref()
                            .map(|expr| self.bind_expr_with_aliases(expr, &extended_schema, &aliases))
                            .transpose()?;
                        Ok(Some(OnConflictAction::DoUpdate(assignments, where_clause)))
                    }
                }
            }
            _ => Err(SqlError::Unsupported("ON DUPLICATE KEY UPDATE".into())),
        }
    }
}
