use crate::codec::{BackendMessage, FieldDescription};
use crate::session::PgSession;

use super::handler::{extract_where_eq, QueryHandler};

impl QueryHandler {
    /// Route information_schema and pg_catalog queries.
    /// Returns `Some(messages)` if handled, `None` otherwise.
    pub(crate) fn handle_catalog_query(
        &self,
        sql_lower: &str,
        _session: &mut PgSession,
    ) -> Option<Vec<BackendMessage>> {
        // information_schema queries
        if sql_lower.contains("information_schema.tables")
            || sql_lower.contains("information_schema.columns")
            || sql_lower.contains("information_schema.schemata")
            || sql_lower.contains("information_schema.table_constraints")
            || sql_lower.contains("information_schema.key_column_usage")
        {
            if sql_lower.contains("information_schema.columns") {
                return Some(self.handle_information_schema_columns(sql_lower));
            }
            if sql_lower.contains("information_schema.tables") {
                return Some(self.handle_information_schema_tables(sql_lower));
            }
            if sql_lower.contains("information_schema.schemata") {
                return Some(self.handle_information_schema_schemata());
            }
            if sql_lower.contains("information_schema.table_constraints") {
                return Some(self.handle_information_schema_table_constraints());
            }
            if sql_lower.contains("information_schema.key_column_usage") {
                return Some(self.handle_information_schema_key_column_usage());
            }
            return Some(self.single_row_result(vec![], vec![]));
        }

        // pg_catalog system table queries (psql \dt, \d, ORMs, etc.)
        if sql_lower.contains("pg_catalog.") {
            // \dt: list tables
            if sql_lower.contains("pg_class") && sql_lower.contains("relkind") {
                return Some(self.handle_list_tables());
            }
            // \d table_name: column info
            if sql_lower.contains("pg_attribute") {
                return Some(self.handle_pg_attribute(sql_lower));
            }
            // pg_type — ORM type mapping
            if sql_lower.contains("pg_type") {
                return Some(self.handle_pg_type(sql_lower));
            }
            // pg_namespace — schema listing
            if sql_lower.contains("pg_namespace") {
                return Some(self.handle_pg_namespace());
            }
            // pg_index — index info for ORMs
            if sql_lower.contains("pg_index") {
                return Some(self.handle_pg_index(sql_lower));
            }
            // pg_constraint — PK/FK/UNIQUE/CHECK constraints
            if sql_lower.contains("pg_constraint") {
                return Some(self.handle_pg_constraint(sql_lower));
            }
            // pg_database — database listing
            if sql_lower.contains("pg_database") {
                return Some(self.handle_pg_database());
            }
            // pg_settings — configuration parameters
            if sql_lower.contains("pg_settings") {
                return Some(self.handle_pg_settings());
            }
            // pg_description — object comments (stub)
            if sql_lower.contains("pg_description") {
                return Some(self.single_row_result(
                    vec![
                        ("objoid", 23, 4),
                        ("classoid", 23, 4),
                        ("objsubid", 23, 4),
                        ("description", 25, -1),
                    ],
                    vec![],
                ));
            }
            // pg_am — access methods (stub for index type queries)
            if sql_lower.contains("pg_am") {
                return Some(self.single_row_result(
                    vec![("oid", 23, 4), ("amname", 25, -1), ("amtype", 18, 1)],
                    vec![
                        vec![Some("403".into()), Some("btree".into()), Some("i".into())],
                        vec![Some("405".into()), Some("hash".into()), Some("i".into())],
                    ],
                ));
            }
            // Fallback: empty result for unhandled catalog queries
            return Some(self.single_row_result(vec![], vec![]));
        }

        None
    }

    /// Build a response for \dt — list user tables.
    fn handle_list_tables(&self) -> Vec<BackendMessage> {
        let catalog = self.storage.get_catalog();
        let tables = catalog.list_tables();
        let table_count = tables.len();

        let fields = vec![
            FieldDescription {
                name: "Schema".into(),
                table_oid: 0,
                column_attr: 0,
                type_oid: 25,
                type_len: -1,
                type_modifier: -1,
                format_code: 0,
            },
            FieldDescription {
                name: "Name".into(),
                table_oid: 0,
                column_attr: 0,
                type_oid: 25,
                type_len: -1,
                type_modifier: -1,
                format_code: 0,
            },
            FieldDescription {
                name: "Type".into(),
                table_oid: 0,
                column_attr: 0,
                type_oid: 25,
                type_len: -1,
                type_modifier: -1,
                format_code: 0,
            },
            FieldDescription {
                name: "Owner".into(),
                table_oid: 0,
                column_attr: 0,
                type_oid: 25,
                type_len: -1,
                type_modifier: -1,
                format_code: 0,
            },
        ];

        let mut messages = vec![BackendMessage::RowDescription { fields }];

        for table in &tables {
            messages.push(BackendMessage::DataRow {
                values: vec![
                    Some("public".into()),
                    Some(table.name.clone()),
                    Some("table".into()),
                    Some("falcon".into()),
                ],
            });
        }

        messages.push(BackendMessage::CommandComplete {
            tag: format!("SELECT {}", table_count),
        });

        messages
    }

    /// information_schema.tables — returns table_catalog, table_schema, table_name, table_type
    fn handle_information_schema_tables(&self, sql_lower: &str) -> Vec<BackendMessage> {
        let catalog = self.storage.get_catalog();
        let tables = catalog.list_tables();

        let cols = vec![
            ("table_catalog", 25, -1i16),
            ("table_schema", 25, -1),
            ("table_name", 25, -1),
            ("table_type", 25, -1),
        ];

        // Optional WHERE table_name = '...' filter
        let filter = extract_where_eq(sql_lower, "table_name");

        let rows: Vec<Vec<Option<String>>> = tables
            .iter()
            .filter(|t| filter.as_ref().is_none_or(|f| t.name.to_lowercase() == *f))
            .map(|t| {
                vec![
                    Some("falcon".into()),
                    Some("public".into()),
                    Some(t.name.clone()),
                    Some("BASE TABLE".into()),
                ]
            })
            .collect();

        self.single_row_result(cols, rows)
    }

    /// information_schema.columns — expanded with column_default, precision, udt_name, identity.
    fn handle_information_schema_columns(&self, sql_lower: &str) -> Vec<BackendMessage> {
        let catalog = self.storage.get_catalog();
        let tables = catalog.list_tables();

        let cols = vec![
            ("table_catalog", 25, -1i16),
            ("table_schema", 25, -1),
            ("table_name", 25, -1),
            ("column_name", 25, -1),
            ("ordinal_position", 23, 4),
            ("column_default", 25, -1),
            ("is_nullable", 25, -1),
            ("data_type", 25, -1),
            ("character_maximum_length", 23, 4),
            ("numeric_precision", 23, 4),
            ("numeric_scale", 23, 4),
            ("datetime_precision", 23, 4),
            ("udt_name", 25, -1),
            ("is_identity", 25, -1),
            ("identity_generation", 25, -1),
        ];

        let filter = extract_where_eq(sql_lower, "table_name");

        let mut rows = Vec::new();
        for t in tables {
            if let Some(ref f) = filter {
                if t.name.to_lowercase() != *f {
                    continue;
                }
            }
            for (i, col) in t.columns.iter().enumerate() {
                let default_str = if col.is_serial {
                    Some(format!("nextval('{}_{}_seq'::regclass)", t.name, col.name))
                } else {
                    col.default_value.as_ref().map(|d| format!("{}", d))
                };
                rows.push(vec![
                    Some("falcon".into()),
                    Some("public".into()),
                    Some(t.name.clone()),
                    Some(col.name.clone()),
                    Some((i + 1).to_string()),
                    default_str,
                    Some(if col.nullable { "YES" } else { "NO" }.into()),
                    Some(col.data_type.pg_type_name().into()),
                    None, // character_maximum_length (Falcon has no VARCHAR(n) yet)
                    col.data_type.numeric_precision().map(|v| v.to_string()),
                    col.data_type.numeric_scale().map(|v| v.to_string()),
                    col.data_type.datetime_precision().map(|v| v.to_string()),
                    Some(col.data_type.pg_udt_name().into()),
                    Some(if col.is_serial { "YES" } else { "NO" }.into()),
                    if col.is_serial {
                        Some("BY DEFAULT".into())
                    } else {
                        None
                    },
                ]);
            }
        }

        self.single_row_result(cols, rows)
    }

    /// information_schema.schemata
    fn handle_information_schema_schemata(&self) -> Vec<BackendMessage> {
        self.single_row_result(
            vec![
                ("catalog_name", 25, -1),
                ("schema_name", 25, -1),
                ("schema_owner", 25, -1),
            ],
            vec![
                vec![
                    Some("falcon".into()),
                    Some("public".into()),
                    Some("falcon".into()),
                ],
                vec![
                    Some("falcon".into()),
                    Some("information_schema".into()),
                    Some("falcon".into()),
                ],
            ],
        )
    }

    /// information_schema.table_constraints — PRIMARY KEY / UNIQUE / FOREIGN KEY / CHECK
    fn handle_information_schema_table_constraints(&self) -> Vec<BackendMessage> {
        let catalog = self.storage.get_catalog();
        let tables = catalog.list_tables();

        let cols = vec![
            ("constraint_catalog", 25, -1i16),
            ("constraint_schema", 25, -1),
            ("constraint_name", 25, -1),
            ("table_name", 25, -1),
            ("constraint_type", 25, -1),
        ];

        let mut rows = Vec::new();
        for t in tables {
            if !t.primary_key_columns.is_empty() {
                rows.push(vec![
                    Some("falcon".into()),
                    Some("public".into()),
                    Some(format!("{}_pkey", t.name)),
                    Some(t.name.clone()),
                    Some("PRIMARY KEY".into()),
                ]);
            }
            for (i, _) in t.unique_constraints.iter().enumerate() {
                rows.push(vec![
                    Some("falcon".into()),
                    Some("public".into()),
                    Some(format!("{}_unique_{}", t.name, i)),
                    Some(t.name.clone()),
                    Some("UNIQUE".into()),
                ]);
            }
            for (i, fk) in t.foreign_keys.iter().enumerate() {
                rows.push(vec![
                    Some("falcon".into()),
                    Some("public".into()),
                    Some(format!("{}_fkey_{}", t.name, i)),
                    Some(t.name.clone()),
                    Some(format!("FOREIGN KEY -> {}", fk.ref_table)),
                ]);
            }
            for (i, _) in t.check_constraints.iter().enumerate() {
                rows.push(vec![
                    Some("falcon".into()),
                    Some("public".into()),
                    Some(format!("{}_check_{}", t.name, i)),
                    Some(t.name.clone()),
                    Some("CHECK".into()),
                ]);
            }
        }

        self.single_row_result(cols, rows)
    }

    /// information_schema.key_column_usage — PK and UNIQUE key columns
    fn handle_information_schema_key_column_usage(&self) -> Vec<BackendMessage> {
        let catalog = self.storage.get_catalog();
        let tables = catalog.list_tables();

        let cols = vec![
            ("constraint_name", 25, -1i16),
            ("table_name", 25, -1),
            ("column_name", 25, -1),
            ("ordinal_position", 23, 4),
        ];

        let mut rows = Vec::new();
        for t in tables {
            for (pos, &col_idx) in t.primary_key_columns.iter().enumerate() {
                rows.push(vec![
                    Some(format!("{}_pkey", t.name)),
                    Some(t.name.clone()),
                    Some(t.columns[col_idx].name.clone()),
                    Some((pos + 1).to_string()),
                ]);
            }
            for (ui, uniq) in t.unique_constraints.iter().enumerate() {
                for (pos, &col_idx) in uniq.iter().enumerate() {
                    rows.push(vec![
                        Some(format!("{}_unique_{}", t.name, ui)),
                        Some(t.name.clone()),
                        Some(t.columns[col_idx].name.clone()),
                        Some((pos + 1).to_string()),
                    ]);
                }
            }
        }

        self.single_row_result(cols, rows)
    }

    /// pg_attribute handler for psql \d table_name
    fn handle_pg_attribute(&self, sql_lower: &str) -> Vec<BackendMessage> {
        let catalog = self.storage.get_catalog();

        // Try to extract table name from the query
        let table_name = extract_where_eq(sql_lower, "relname");
        if let Some(ref name) = table_name {
            if let Some(schema) = catalog.find_table(name) {
                let cols = vec![
                    ("column_name", 25, -1i16),
                    ("data_type", 25, -1),
                    ("is_nullable", 25, -1),
                    ("column_default", 25, -1),
                ];
                let rows: Vec<Vec<Option<String>>> = schema
                    .columns
                    .iter()
                    .map(|col| {
                        vec![
                            Some(col.name.clone()),
                            Some(col.data_type.pg_type_name().into()),
                            Some(if col.nullable { "YES" } else { "NO" }.into()),
                            col.default_value.as_ref().map(|d| format!("{}", d)),
                        ]
                    })
                    .collect();
                return self.single_row_result(cols, rows);
            }
        }

        // Fallback: empty result
        self.single_row_result(vec![], vec![])
    }

    /// pg_catalog.pg_type — type OID mapping for ORMs and drivers.
    fn handle_pg_type(&self, sql_lower: &str) -> Vec<BackendMessage> {
        // Static list of Falcon-supported types mapped to PG OIDs.
        let builtin_types: Vec<(i32, &str, i16, &str, &str)> = vec![
            // (oid, typname, typlen, typtype, typnamespace_nspname)
            (16, "bool", 1, "b", "pg_catalog"),
            (20, "int8", 8, "b", "pg_catalog"),
            (23, "int4", 4, "b", "pg_catalog"),
            (25, "text", -1, "b", "pg_catalog"),
            (701, "float8", 8, "b", "pg_catalog"),
            (1082, "date", 4, "b", "pg_catalog"),
            (1114, "timestamp", 8, "b", "pg_catalog"),
            (3802, "jsonb", -1, "b", "pg_catalog"),
            (1007, "_int4", -1, "b", "pg_catalog"),
            (1016, "_int8", -1, "b", "pg_catalog"),
            (1009, "_text", -1, "b", "pg_catalog"),
            (1022, "_float8", -1, "b", "pg_catalog"),
            (1000, "_bool", -1, "b", "pg_catalog"),
            (2277, "anyarray", -1, "p", "pg_catalog"),
        ];

        let filter_oid = extract_where_eq(sql_lower, "oid").and_then(|s| s.parse::<i32>().ok());
        let filter_name = extract_where_eq(sql_lower, "typname");

        let cols = vec![
            ("oid", 23, 4i16),
            ("typname", 25, -1),
            ("typlen", 21, 2),
            ("typtype", 18, 1),
            ("typnamespace", 23, 4),
            ("typnotnull", 16, 1),
        ];

        let rows: Vec<Vec<Option<String>>> = builtin_types
            .iter()
            .filter(|(oid, name, _, _, _)| {
                filter_oid.as_ref().is_none_or(|f| f == oid)
                    && filter_name.as_ref().is_none_or(|f| f == name)
            })
            .map(|(oid, name, len, typtype, _)| {
                vec![
                    Some(oid.to_string()),
                    Some(name.to_string()),
                    Some(len.to_string()),
                    Some(typtype.to_string()),
                    Some("11".into()), // pg_catalog namespace OID
                    Some("f".into()),
                ]
            })
            .collect();

        self.single_row_result(cols, rows)
    }

    /// pg_catalog.pg_namespace — schema listing.
    fn handle_pg_namespace(&self) -> Vec<BackendMessage> {
        let cols = vec![("oid", 23, 4i16), ("nspname", 25, -1), ("nspowner", 23, 4)];
        let rows = vec![
            vec![
                Some("11".into()),
                Some("pg_catalog".into()),
                Some("10".into()),
            ],
            vec![
                Some("2200".into()),
                Some("public".into()),
                Some("10".into()),
            ],
            vec![
                Some("12052".into()),
                Some("information_schema".into()),
                Some("10".into()),
            ],
        ];
        self.single_row_result(cols, rows)
    }

    /// pg_catalog.pg_index — index information for ORMs.
    /// Exposes primary key as a btree index per table.
    fn handle_pg_index(&self, sql_lower: &str) -> Vec<BackendMessage> {
        let catalog = self.storage.get_catalog();
        let tables = catalog.list_tables();

        let filter_rel = extract_where_eq(sql_lower, "indrelid");

        let cols = vec![
            ("indexrelid", 23, 4i16),
            ("indrelid", 23, 4),
            ("indnatts", 21, 2),
            ("indisunique", 16, 1),
            ("indisprimary", 16, 1),
            ("indkey", 25, -1),
        ];

        let mut rows: Vec<Vec<Option<String>>> = Vec::new();
        let mut fake_idx_oid = 90000i64;

        for table in tables {
            let table_oid = table.id.0 as i64 + 16384;
            if let Some(ref f) = filter_rel {
                if f.parse::<i64>().ok() != Some(table_oid) {
                    continue;
                }
            }
            // PK index
            if !table.primary_key_columns.is_empty() {
                let indkey = table
                    .primary_key_columns
                    .iter()
                    .map(|i| (*i + 1).to_string())
                    .collect::<Vec<_>>()
                    .join(" ");
                rows.push(vec![
                    Some(fake_idx_oid.to_string()),
                    Some(table_oid.to_string()),
                    Some(table.primary_key_columns.len().to_string()),
                    Some("t".into()),
                    Some("t".into()),
                    Some(indkey),
                ]);
                fake_idx_oid += 1;
            }
            // UNIQUE constraint indexes
            for uc in &table.unique_constraints {
                let indkey = uc
                    .iter()
                    .map(|i| (*i + 1).to_string())
                    .collect::<Vec<_>>()
                    .join(" ");
                rows.push(vec![
                    Some(fake_idx_oid.to_string()),
                    Some(table_oid.to_string()),
                    Some(uc.len().to_string()),
                    Some("t".into()),
                    Some("f".into()),
                    Some(indkey),
                ]);
                fake_idx_oid += 1;
            }
        }

        self.single_row_result(cols, rows)
    }

    /// pg_catalog.pg_constraint — PK, FK, UNIQUE, CHECK constraints.
    fn handle_pg_constraint(&self, sql_lower: &str) -> Vec<BackendMessage> {
        let catalog = self.storage.get_catalog();
        let tables = catalog.list_tables();

        let filter_rel = extract_where_eq(sql_lower, "conrelid");

        let cols = vec![
            ("oid", 23, 4i16),
            ("conname", 25, -1),
            ("connamespace", 23, 4),
            ("contype", 18, 1), // p=PK, u=unique, f=FK, c=check
            ("conrelid", 23, 4),
            ("confrelid", 23, 4), // 0 for non-FK
            ("conkey", 25, -1),   // array of column numbers
            ("confkey", 25, -1),  // array of FK referenced column numbers
        ];

        let mut rows: Vec<Vec<Option<String>>> = Vec::new();
        let mut fake_oid = 80000i64;

        for table in &tables {
            let table_oid = table.id.0 as i64 + 16384;
            if let Some(ref f) = filter_rel {
                if f.parse::<i64>().ok() != Some(table_oid) {
                    continue;
                }
            }

            // PK constraint
            if !table.primary_key_columns.is_empty() {
                let conkey = format!(
                    "{{{}}}",
                    table
                        .primary_key_columns
                        .iter()
                        .map(|i: &usize| (i + 1).to_string())
                        .collect::<Vec<_>>()
                        .join(",")
                );
                rows.push(vec![
                    Some(fake_oid.to_string()),
                    Some(format!("{}_pkey", table.name)),
                    Some("2200".into()),
                    Some("p".into()),
                    Some(table_oid.to_string()),
                    Some("0".into()),
                    Some(conkey),
                    None,
                ]);
                fake_oid += 1;
            }

            // UNIQUE constraints
            for (i, uc) in table.unique_constraints.iter().enumerate() {
                let conkey_vals: Vec<String> = uc.iter().map(|c| (*c + 1).to_string()).collect();
                let conkey = format!("{{{}}}", conkey_vals.join(","));
                rows.push(vec![
                    Some(fake_oid.to_string()),
                    Some(format!("{}_unique_{}", table.name, i + 1)),
                    Some("2200".into()),
                    Some("u".into()),
                    Some(table_oid.to_string()),
                    Some("0".into()),
                    Some(conkey),
                    None,
                ]);
                fake_oid += 1;
            }

            // FK constraints
            for (i, fk) in table.foreign_keys.iter().enumerate() {
                let conkey = format!(
                    "{{{}}}",
                    fk.columns
                        .iter()
                        .map(|c: &usize| (c + 1).to_string())
                        .collect::<Vec<_>>()
                        .join(",")
                );
                // Resolve referenced table OID
                let ref_oid = tables
                    .iter()
                    .find(|t| t.name.to_lowercase() == fk.ref_table.to_lowercase())
                    .map(|t| t.id.0 as i64 + 16384)
                    .unwrap_or(0);
                // Resolve referenced column numbers
                let confkey = if ref_oid > 0 {
                    let ref_table = tables
                        .iter()
                        .find(|t| t.name.to_lowercase() == fk.ref_table.to_lowercase());
                    if let Some(rt) = ref_table {
                        let indices: Vec<String> = fk
                            .ref_columns
                            .iter()
                            .filter_map(|rc| rt.find_column(rc).map(|i| (i + 1).to_string()))
                            .collect();
                        Some(format!("{{{}}}", indices.join(",")))
                    } else {
                        None
                    }
                } else {
                    None
                };
                rows.push(vec![
                    Some(fake_oid.to_string()),
                    Some(format!("{}_fk_{}", table.name, i + 1)),
                    Some("2200".into()),
                    Some("f".into()),
                    Some(table_oid.to_string()),
                    Some(ref_oid.to_string()),
                    Some(conkey),
                    confkey,
                ]);
                fake_oid += 1;
            }

            // CHECK constraints
            for (i, _expr) in table.check_constraints.iter().enumerate() {
                rows.push(vec![
                    Some(fake_oid.to_string()),
                    Some(format!("{}_check_{}", table.name, i + 1)),
                    Some("2200".into()),
                    Some("c".into()),
                    Some(table_oid.to_string()),
                    Some("0".into()),
                    None,
                    None,
                ]);
                fake_oid += 1;
            }
        }

        self.single_row_result(cols, rows)
    }

    /// pg_catalog.pg_database — database listing.
    fn handle_pg_database(&self) -> Vec<BackendMessage> {
        let cols = vec![
            ("oid", 23, 4i16),
            ("datname", 25, -1),
            ("datdba", 23, 4),
            ("encoding", 23, 4),
            ("datcollate", 25, -1),
            ("datctype", 25, -1),
        ];
        let rows = vec![vec![
            Some("16384".into()),
            Some("falcon".into()),
            Some("10".into()),
            Some("6".into()), // UTF8
            Some("en_US.UTF-8".into()),
            Some("en_US.UTF-8".into()),
        ]];
        self.single_row_result(cols, rows)
    }

    /// pg_catalog.pg_settings — configuration parameters.
    fn handle_pg_settings(&self) -> Vec<BackendMessage> {
        let cols = vec![
            ("name", 25, -1i16),
            ("setting", 25, -1),
            ("unit", 25, -1),
            ("category", 25, -1),
            ("short_desc", 25, -1),
        ];
        let rows = vec![
            vec![
                Some("server_version".into()),
                Some("15.0".into()),
                None,
                Some("Version".into()),
                Some("FalconDB PG-compatible version".into()),
            ],
            vec![
                Some("server_encoding".into()),
                Some("UTF8".into()),
                None,
                Some("Client".into()),
                Some("Server character set encoding".into()),
            ],
            vec![
                Some("client_encoding".into()),
                Some("UTF8".into()),
                None,
                Some("Client".into()),
                Some("Client character set encoding".into()),
            ],
            vec![
                Some("is_superuser".into()),
                Some("on".into()),
                None,
                Some("Auth".into()),
                Some("Current user is superuser".into()),
            ],
            vec![
                Some("DateStyle".into()),
                Some("ISO, MDY".into()),
                None,
                Some("Client".into()),
                Some("Date display format".into()),
            ],
            vec![
                Some("IntervalStyle".into()),
                Some("postgres".into()),
                None,
                Some("Client".into()),
                Some("Interval display format".into()),
            ],
            vec![
                Some("TimeZone".into()),
                Some("UTC".into()),
                None,
                Some("Client".into()),
                Some("Current time zone".into()),
            ],
            vec![
                Some("integer_datetimes".into()),
                Some("on".into()),
                None,
                Some("Preset".into()),
                Some("Datetimes are integer based".into()),
            ],
            vec![
                Some("standard_conforming_strings".into()),
                Some("on".into()),
                None,
                Some("Client".into()),
                Some("Backslash handling in strings".into()),
            ],
            vec![
                Some("max_connections".into()),
                Some("100".into()),
                None,
                Some("Connections".into()),
                Some("Max concurrent connections".into()),
            ],
            vec![
                Some("search_path".into()),
                Some("\"$user\", public".into()),
                None,
                Some("Client".into()),
                Some("Schema search order".into()),
            ],
            vec![
                Some("default_transaction_isolation".into()),
                Some("read committed".into()),
                None,
                Some("Client".into()),
                Some("Default isolation level".into()),
            ],
        ];
        self.single_row_result(cols, rows)
    }
}
