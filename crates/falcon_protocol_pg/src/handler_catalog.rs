use crate::codec::{BackendMessage, FieldDescription};
use crate::session::{PgSession, PG_COMPAT_VERSION, PG_COMPAT_VERSION_NUM};

use crate::handler::QueryHandler;
use crate::handler_utils::extract_where_eq;

impl QueryHandler {
    /// Route information_schema and pg_catalog queries.
    /// Returns `Some(messages)` if handled, `None` otherwise.
    pub(crate) fn handle_catalog_query(
        &self,
        sql_lower: &str,
        _session: &mut PgSession,
    ) -> Option<Vec<BackendMessage>> {
        // information_schema queries
        if sql_lower.contains("information_schema.") {
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
            if sql_lower.contains("information_schema.referential_constraints") {
                return Some(self.handle_information_schema_referential_constraints());
            }
            if sql_lower.contains("information_schema.constraint_column_usage") {
                return Some(self.handle_information_schema_constraint_column_usage());
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
            // pg_stat_user_tables — table statistics (Hibernate/Spring query this)
            if sql_lower.contains("pg_stat_user_tables") {
                return Some(self.handle_pg_stat_user_tables());
            }
            // pg_statio_user_tables — table I/O statistics (ORM startup probe)
            if sql_lower.contains("pg_statio_user_tables") {
                return Some(self.handle_pg_statio_user_tables());
            }
            // pg_class — generic (without relkind filter handled above)
            if sql_lower.contains("pg_class") {
                return Some(self.handle_pg_class(sql_lower));
            }
            // pg_proc — function/procedure listing (ORM introspection)
            if sql_lower.contains("pg_proc") {
                return Some(self.single_row_result(
                    vec![
                        ("oid", 23, 4),
                        ("proname", 25, -1),
                        ("pronamespace", 23, 4),
                        ("prorettype", 23, 4),
                    ],
                    vec![],
                ));
            }
            // pg_enum — enum type values (ORM introspection)
            if sql_lower.contains("pg_enum") {
                return Some(self.single_row_result(
                    vec![
                        ("oid", 23, 4),
                        ("enumtypid", 23, 4),
                        ("enumsortorder", 701, 8),
                        ("enumlabel", 25, -1),
                    ],
                    vec![],
                ));
            }
            // pg_stat_database — database-level aggregate stats
            if sql_lower.contains("pg_stat_database") {
                return Some(self.handle_pg_stat_database(_session));
            }
            // pg_stat_bgwriter — background writer stats
            if sql_lower.contains("pg_stat_bgwriter") {
                return Some(self.handle_pg_stat_bgwriter());
            }
            // pg_stat_activity — active session info
            if sql_lower.contains("pg_stat_activity") {
                return Some(self.handle_pg_stat_activity(_session));
            }
            // pg_locks — lock info
            if sql_lower.contains("pg_locks") {
                return Some(self.handle_pg_locks());
            }
            // pg_prepared_xacts — prepared transactions (stub)
            if sql_lower.contains("pg_prepared_xacts") {
                return Some(self.single_row_result(
                    vec![
                        ("transaction", 23, 4),
                        ("gid", 25, -1),
                        ("prepared", 1114, 8),
                        ("owner", 25, -1),
                        ("database", 25, -1),
                    ],
                    vec![],
                ));
            }
            // pg_tables — table listing (used by Hibernate and some ORMs)
            if sql_lower.contains("pg_tables") {
                return Some(self.handle_pg_tables(sql_lower));
            }
            // pg_sequences — sequence info (Hibernate schema validation)
            if sql_lower.contains("pg_sequences") {
                return Some(self.handle_pg_sequences());
            }
            // pg_inherits — table inheritance (Hibernate schema tool)
            if sql_lower.contains("pg_inherits") {
                return Some(self.single_row_result(
                    vec![("inhrelid", 26, 4i16), ("inhparent", 26, 4), ("inhseqno", 23, 4)],
                    vec![],
                ));
            }
            // pg_roles / pg_user
            if sql_lower.contains("pg_roles") || sql_lower.contains("pg_user") {
                return Some(self.handle_pg_roles());
            }
            // pg_attrdef — column defaults (Hibernate, Django, SQLAlchemy)
            if sql_lower.contains("pg_attrdef") {
                return Some(self.single_row_result(
                    vec![("oid", 26, 4i16), ("adrelid", 26, 4), ("adnum", 21, 2), ("adbin", 25, -1)],
                    vec![],
                ));
            }
            // pg_depend — object dependencies (pg_dump, ORMs)
            if sql_lower.contains("pg_depend") {
                return Some(self.single_row_result(
                    vec![
                        ("classid", 26, 4i16), ("objid", 26, 4), ("objsubid", 23, 4),
                        ("refclassid", 26, 4), ("refobjid", 26, 4), ("refobjsubid", 23, 4),
                        ("deptype", 18, 1),
                    ],
                    vec![],
                ));
            }
            // pg_collation — collation info
            if sql_lower.contains("pg_collation") {
                return Some(self.single_row_result(
                    vec![
                        ("oid", 26, 4i16), ("collname", 25, -1), ("collnamespace", 26, 4),
                        ("collowner", 26, 4), ("collencoding", 23, 4),
                    ],
                    vec![
                        vec![Some("100".into()), Some("default".into()), Some("11".into()), Some("10".into()), Some("-1".into())],
                        vec![Some("950".into()), Some("C".into()), Some("11".into()), Some("10".into()), Some("-1".into())],
                        vec![Some("951".into()), Some("POSIX".into()), Some("11".into()), Some("10".into()), Some("-1".into())],
                    ],
                ));
            }
            // pg_extension — installed extensions
            if sql_lower.contains("pg_extension") {
                return Some(self.single_row_result(
                    vec![
                        ("oid", 26, 4i16), ("extname", 25, -1), ("extowner", 26, 4),
                        ("extnamespace", 26, 4), ("extversion", 25, -1),
                    ],
                    vec![
                        vec![Some("13823".into()), Some("plpgsql".into()), Some("10".into()), Some("11".into()), Some("1.0".into())],
                    ],
                ));
            }
            // pg_range — range type info
            if sql_lower.contains("pg_range") {
                return Some(self.single_row_result(
                    vec![
                        ("rngtypid", 26, 4i16), ("rngsubtype", 26, 4), ("rngmultitypid", 26, 4),
                        ("rngcollation", 26, 4), ("rngsubopc", 26, 4),
                    ],
                    vec![],
                ));
            }
            // Fallback: empty result for unhandled catalog queries
            return Some(self.single_row_result(vec![], vec![]));
        }

        // Unqualified pg_stat_activity / pg_locks (used by falcon_cli fallback queries)
        if sql_lower.contains("pg_stat_activity") {
            return Some(self.handle_pg_stat_activity(_session));
        }
        if sql_lower.contains("pg_locks") {
            return Some(self.handle_pg_locks());
        }
        if sql_lower.contains("pg_prepared_xacts") {
            return Some(self.single_row_result(
                vec![
                    ("transaction", 23, 4),
                    ("gid", 25, -1),
                    ("prepared", 1114, 8),
                    ("owner", 25, -1),
                    ("database", 25, -1),
                ],
                vec![],
            ));
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
            tag: format!("SELECT {table_count}"),
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
                    col.default_value.as_ref().map(|d| format!("{d}"))
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
                            col.default_value.as_ref().map(|d| format!("{d}")),
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
        // (typname, oid, typarray, typlen, typtype)
        // Column order matches pgjdbc TypeInfoCache expectation: col1=typname, col2=oid, col3=typarray
        let builtin_types: &[(&str, i32, i32, i16, &str)] = &[
            ("bool",          16,   1000,   1, "b"),
            ("bytea",         17,   1001,  -1, "b"),
            ("char",          18,   1002,   1, "b"),
            ("name",          19,   1003,  64, "b"),
            ("int8",          20,   1016,   8, "b"),
            ("int2",          21,   1005,   2, "b"),
            ("int4",          23,   1007,   4, "b"),
            ("text",          25,   1009,  -1, "b"),
            ("oid",           26,   1028,   4, "b"),
            ("json",         114,    199,  -1, "b"),
            ("xml",          142,    143,  -1, "b"),
            ("float4",       700,   1021,   4, "b"),
            ("float8",       701,   1022,   8, "b"),
            ("money",        790,    791,   8, "b"),
            ("bpchar",      1042,   1014,  -1, "b"),
            ("varchar",     1043,   1015,  -1, "b"),
            ("date",        1082,   1182,   4, "b"),
            ("time",        1083,   1183,   8, "b"),
            ("timestamp",   1114,   1115,   8, "b"),
            ("timestamptz", 1184,   1185,   8, "b"),
            ("interval",    1186,   1187,  16, "b"),
            ("timetz",      1266,   1270,  12, "b"),
            ("bit",         1560,   1561,  -1, "b"),
            ("varbit",      1562,   1563,  -1, "b"),
            ("numeric",     1700,   1231,  -1, "b"),
            ("void",        2278,      0,   4, "p"),
            ("uuid",        2950,   2951,  16, "b"),
            ("jsonb",       3802,   3807,  -1, "b"),
            // array types
            ("_bool",       1000,      0,  -1, "b"),
            ("_bytea",      1001,      0,  -1, "b"),
            ("_char",       1002,      0,  -1, "b"),
            ("_name",       1003,      0,  -1, "b"),
            ("_int2",       1005,      0,  -1, "b"),
            ("_int4",       1007,      0,  -1, "b"),
            ("_int8",       1016,      0,  -1, "b"),
            ("_text",       1009,      0,  -1, "b"),
            ("_oid",        1028,      0,  -1, "b"),
            ("_float4",     1021,      0,  -1, "b"),
            ("_float8",     1022,      0,  -1, "b"),
            ("_bpchar",     1014,      0,  -1, "b"),
            ("_varchar",    1015,      0,  -1, "b"),
            ("_date",       1182,      0,  -1, "b"),
            ("_time",       1183,      0,  -1, "b"),
            ("_timestamp",  1115,      0,  -1, "b"),
            ("_timestamptz",1185,      0,  -1, "b"),
            ("_interval",   1187,      0,  -1, "b"),
            ("_numeric",    1231,      0,  -1, "b"),
            ("_uuid",       2951,      0,  -1, "b"),
            ("_jsonb",      3807,      0,  -1, "b"),
            ("anyarray",    2277,      0,  -1, "p"),
        ];

        let filter_oid = extract_where_eq(sql_lower, "oid").and_then(|s| s.parse::<i32>().ok());
        let filter_name = extract_where_eq(sql_lower, "typname");

        let cols = vec![
            ("typname", 25, -1i16),
            ("oid", 23, 4),
            ("typarray", 23, 4),
            ("typlen", 21, 2),
            ("typtype", 18, 1),
            ("typnamespace", 23, 4),
            ("typnotnull", 16, 1),
            ("typbasetype", 23, 4),
        ];

        let rows: Vec<Vec<Option<String>>> = builtin_types
            .iter()
            .filter(|(name, oid, _, _, _)| {
                filter_oid.as_ref().is_none_or(|f| f == oid)
                    && filter_name.as_ref().is_none_or(|f| f == name)
            })
            .map(|(name, oid, typarray, len, typtype)| {
                vec![
                    Some(name.to_string()),
                    Some(oid.to_string()),
                    Some(typarray.to_string()),
                    Some(len.to_string()),
                    Some(typtype.to_string()),
                    Some("11".into()),
                    Some("f".into()),
                    Some("0".into()),
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
                    .map_or(0, |t| t.id.0 as i64 + 16384);
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
        let catalog = self.storage.get_catalog();
        let databases = catalog.list_databases();
        let rows: Vec<Vec<Option<String>>> = databases
            .iter()
            .map(|db| {
                vec![
                    Some(db.oid.to_string()),
                    Some(db.name.clone()),
                    Some("10".into()),
                    Some("6".into()), // UTF8
                    Some("en_US.UTF-8".into()),
                    Some("en_US.UTF-8".into()),
                ]
            })
            .collect();
        drop(catalog);
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
                Some(PG_COMPAT_VERSION.into()),
                None,
                Some("Version".into()),
                Some("FalconDB PG-compatible version".into()),
            ],
            vec![
                Some("server_version_num".into()),
                Some(PG_COMPAT_VERSION_NUM.into()),
                None,
                Some("Version".into()),
                Some("FalconDB PG-compatible version number".into()),
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

    /// pg_stat_user_tables — table-level statistics (Hibernate/Spring probe this on startup).
    fn handle_pg_stat_user_tables(&self) -> Vec<BackendMessage> {
        let catalog = self.storage.get_catalog();
        let tables = catalog.list_tables();

        let cols = vec![
            ("relid", 23, 4i16),
            ("schemaname", 25, -1),
            ("relname", 25, -1),
            ("seq_scan", 20, 8),
            ("seq_tup_read", 20, 8),
            ("idx_scan", 20, 8),
            ("idx_tup_fetch", 20, 8),
            ("n_tup_ins", 20, 8),
            ("n_tup_upd", 20, 8),
            ("n_tup_del", 20, 8),
            ("n_live_tup", 20, 8),
            ("n_dead_tup", 20, 8),
        ];

        let rows: Vec<Vec<Option<String>>> = tables
            .iter()
            .map(|t| {
                let oid = (t.id.0 as i64 + 16384).to_string();
                vec![
                    Some(oid),
                    Some("public".into()),
                    Some(t.name.clone()),
                    Some("0".into()), // seq_scan
                    Some("0".into()), // seq_tup_read
                    Some("0".into()), // idx_scan
                    Some("0".into()), // idx_tup_fetch
                    Some("0".into()), // n_tup_ins
                    Some("0".into()), // n_tup_upd
                    Some("0".into()), // n_tup_del
                    Some("0".into()), // n_live_tup
                    Some("0".into()), // n_dead_tup
                ]
            })
            .collect();

        self.single_row_result(cols, rows)
    }

    /// pg_statio_user_tables — table I/O statistics (ORM startup probe).
    fn handle_pg_statio_user_tables(&self) -> Vec<BackendMessage> {
        let catalog = self.storage.get_catalog();
        let tables = catalog.list_tables();

        let cols = vec![
            ("relid", 23, 4i16),
            ("schemaname", 25, -1),
            ("relname", 25, -1),
            ("heap_blks_read", 20, 8),
            ("heap_blks_hit", 20, 8),
            ("idx_blks_read", 20, 8),
            ("idx_blks_hit", 20, 8),
        ];

        let rows: Vec<Vec<Option<String>>> = tables
            .iter()
            .map(|t| {
                let oid = (t.id.0 as i64 + 16384).to_string();
                vec![
                    Some(oid),
                    Some("public".into()),
                    Some(t.name.clone()),
                    Some("0".into()),
                    Some("0".into()),
                    Some("0".into()),
                    Some("0".into()),
                ]
            })
            .collect();

        self.single_row_result(cols, rows)
    }

    /// pg_catalog.pg_class — generic handler for pg_class queries without relkind filter.
    fn handle_pg_class(&self, sql_lower: &str) -> Vec<BackendMessage> {
        let catalog = self.storage.get_catalog();
        let tables = catalog.list_tables();

        let filter_name = extract_where_eq(sql_lower, "relname");
        let filter_oid = extract_where_eq(sql_lower, "oid");

        let cols = vec![
            ("oid", 23, 4i16),
            ("relname", 25, -1),
            ("relnamespace", 23, 4),
            ("relkind", 18, 1),
            ("reltuples", 701, 8),
            ("relpages", 23, 4),
            ("relhasindex", 16, 1),
            ("relowner", 23, 4),
        ];

        let mut rows: Vec<Vec<Option<String>>> = Vec::new();
        for t in &tables {
            let oid = (t.id.0 as i64 + 16384).to_string();
            if let Some(ref f) = filter_name {
                if t.name.to_lowercase() != *f {
                    continue;
                }
            }
            if let Some(ref f) = filter_oid {
                if *f != oid {
                    continue;
                }
            }
            let has_index = if !t.primary_key_columns.is_empty() {
                "t"
            } else {
                "f"
            };
            rows.push(vec![
                Some(oid),
                Some(t.name.clone()),
                Some("2200".into()), // public namespace
                Some("r".into()),    // ordinary table
                Some("0".into()),    // reltuples (unknown)
                Some("0".into()),    // relpages
                Some(has_index.into()),
                Some("10".into()), // owner OID
            ]);
        }

        self.single_row_result(cols, rows)
    }

    /// pg_catalog.pg_stat_activity — live session info from the session registry.
    fn handle_pg_stat_activity(&self, session: &PgSession) -> Vec<BackendMessage> {
        let cols = vec![
            ("datid", 26, 4i16),      // oid
            ("datname", 25, -1),
            ("pid", 23, 4),
            ("usesysid", 26, 4),
            ("usename", 25, -1),
            ("application_name", 25, -1),
            ("client_addr", 869, -1),  // inet
            ("client_hostname", 25, -1),
            ("client_port", 23, 4),
            ("backend_start", 1184, 8),
            ("xact_start", 1184, 8),
            ("query_start", 1184, 8),
            ("state_change", 1184, 8),
            ("wait_event_type", 25, -1),
            ("wait_event", 25, -1),
            ("state", 25, -1),
            ("backend_xid", 28, 4),
            ("backend_xmin", 28, 4),
            ("query", 25, -1),
            ("backend_type", 25, -1),
        ];

        let snapshots = self.session_registry.snapshot();
        let db = &session.database;
        let mut rows: Vec<Vec<Option<String>>> = Vec::with_capacity(snapshots.len());
        for s in &snapshots {
            let (addr, port) = if let Some(pos) = s.client_addr.rfind(':') {
                (s.client_addr[..pos].to_string(), s.client_addr[pos+1..].to_string())
            } else {
                (s.client_addr.clone(), "0".into())
            };
            rows.push(vec![
                Some("16384".into()),          // datid
                Some(db.clone()),              // datname
                Some(s.pid.to_string()),
                Some("10".into()),             // usesysid
                Some(s.user.clone()),
                Some(s.application_name.clone()),
                Some(addr),
                None,                          // client_hostname
                Some(port),
                None,                          // backend_start (no wall-clock yet)
                s.xact_start_secs.map(|_| String::new()), // placeholder
                s.query_start_secs.map(|_| String::new()),
                None,                          // state_change
                None, None,                    // wait_event_type, wait_event
                Some(s.state.to_string()),
                None, None,                    // backend_xid, backend_xmin
                Some(s.query.clone()),
                Some("client backend".into()),
            ]);
        }

        self.single_row_result(cols, rows)
    }

    /// pg_catalog.pg_locks — expose MVCC write-set locks from active transactions.
    fn handle_pg_locks(&self) -> Vec<BackendMessage> {
        let cols = vec![
            ("locktype", 25, -1i16),
            ("database", 26, 4),
            ("relation", 26, 4),
            ("page", 23, 4),
            ("tuple", 21, 2),
            ("virtualxid", 25, -1),
            ("transactionid", 28, 4),
            ("classid", 26, 4),
            ("objid", 26, 4),
            ("objsubid", 21, 2),
            ("virtualtransaction", 25, -1),
            ("pid", 23, 4),
            ("mode", 25, -1),
            ("granted", 16, 1),
            ("fastpath", 16, 1),
            ("waitstart", 1184, 8),
        ];

        // Snapshot active transactions from the txn manager
        let snapshots = self.session_registry.snapshot();
        let mut rows: Vec<Vec<Option<String>>> = Vec::new();
        for s in &snapshots {
            // Each connected session with state != idle holds a virtualxid lock
            rows.push(vec![
                Some("virtualxid".into()),   // locktype
                None,                         // database
                None,                         // relation
                None, None,                   // page, tuple
                Some(format!("{}/1", s.pid)), // virtualxid
                None,                         // transactionid
                None, None, None,             // classid, objid, objsubid
                Some(format!("{}/1", s.pid)), // virtualtransaction
                Some(s.pid.to_string()),
                Some("ExclusiveLock".into()),
                Some("t".into()),             // granted
                Some("t".into()),             // fastpath
                None,                         // waitstart
            ]);
        }

        self.single_row_result(cols, rows)
    }

    fn handle_pg_tables(&self, sql_lower: &str) -> Vec<BackendMessage> {
        let catalog = self.storage.get_catalog();
        let tables = catalog.list_tables();
        let filter = extract_where_eq(sql_lower, "tablename")
            .or_else(|| extract_where_eq(sql_lower, "schemaname"));
        let cols = vec![
            ("schemaname", 25, -1i16),
            ("tablename", 25, -1),
            ("tableowner", 25, -1),
            ("hasindexes", 16, 1),
            ("hasrules", 16, 1),
            ("hastriggers", 16, 1),
            ("rowsecurity", 16, 1),
        ];
        let rows: Vec<Vec<Option<String>>> = tables
            .iter()
            .filter(|t| filter.as_ref().is_none_or(|f| t.name.to_lowercase() == *f))
            .map(|t| vec![
                Some("public".into()),
                Some(t.name.clone()),
                Some("falcon".into()),
                Some("t".into()),
                Some("f".into()),
                Some("f".into()),
                Some("f".into()),
            ])
            .collect();
        self.single_row_result(cols, rows)
    }

    fn handle_pg_sequences(&self) -> Vec<BackendMessage> {
        let catalog = self.storage.get_catalog();
        let tables = catalog.list_tables();
        let cols = vec![
            ("schemaname", 25, -1i16),
            ("sequencename", 25, -1),
            ("data_type", 25, -1),
            ("start_value", 20, 8),
            ("minimum_value", 20, 8),
            ("maximum_value", 20, 8),
            ("increment", 20, 8),
            ("cycle_option", 25, -1),
            ("cache_size", 20, 8),
        ];
        let mut rows = Vec::new();
        for t in &tables {
            for col in &t.columns {
                if col.is_serial {
                    rows.push(vec![
                        Some("public".into()),
                        Some(format!("{}_{}_seq", t.name, col.name)),
                        Some("bigint".into()),
                        Some("1".into()),
                        Some("1".into()),
                        Some("9223372036854775807".into()),
                        Some("1".into()),
                        Some("NO".into()),
                        Some("1".into()),
                    ]);
                }
            }
        }
        self.single_row_result(cols, rows)
    }

    fn handle_pg_roles(&self) -> Vec<BackendMessage> {
        let cols = vec![
            ("oid", 26, 4i16),
            ("rolname", 25, -1),
            ("rolsuper", 16, 1),
            ("rolinherit", 16, 1),
            ("rolcreaterole", 16, 1),
            ("rolcreatedb", 16, 1),
            ("rolcanlogin", 16, 1),
            ("rolreplication", 16, 1),
            ("rolconnlimit", 23, 4),
            ("rolpassword", 25, -1),
            ("rolvaliduntil", 1114, 8),
            ("rolbypassrls", 16, 1),
        ];
        let rows = vec![vec![
            Some("10".into()),
            Some("falcon".into()),
            Some("t".into()),
            Some("t".into()),
            Some("t".into()),
            Some("t".into()),
            Some("t".into()),
            Some("t".into()),
            Some("-1".into()),
            None,
            None,
            Some("f".into()),
        ]];
        self.single_row_result(cols, rows)
    }

    /// pg_stat_database — database-level aggregate statistics.
    fn handle_pg_stat_database(&self, session: &PgSession) -> Vec<BackendMessage> {
        use std::sync::atomic::Ordering::Relaxed;
        let ds = falcon_common::globals::db_stats();
        let cols = vec![
            ("datid", 26, 4i16),
            ("datname", 25, -1),
            ("numbackends", 23, 4),
            ("xact_commit", 20, 8),
            ("xact_rollback", 20, 8),
            ("blks_read", 20, 8),
            ("blks_hit", 20, 8),
            ("tup_returned", 20, 8),
            ("tup_fetched", 20, 8),
            ("tup_inserted", 20, 8),
            ("tup_updated", 20, 8),
            ("tup_deleted", 20, 8),
            ("temp_files", 20, 8),
            ("temp_bytes", 20, 8),
            ("deadlocks", 20, 8),
            ("blk_read_time", 701, 8),
            ("blk_write_time", 701, 8),
            ("session_time", 701, 8),
            ("active_time", 701, 8),
            ("sessions", 20, 8),
            ("stats_reset", 1184, 8),
        ];
        let num_backends = self.session_registry.snapshot().len();
        let active_ms = ds.active_time_us.load(Relaxed) as f64 / 1000.0;
        let uptime_ms = falcon_common::globals::server_uptime_secs() * 1000.0;
        let rows = vec![vec![
            Some("16384".into()),
            Some(session.database.clone()),
            Some(num_backends.to_string()),
            Some(ds.xact_commit.load(Relaxed).to_string()),
            Some(ds.xact_rollback.load(Relaxed).to_string()),
            Some(ds.blks_read.load(Relaxed).to_string()),
            Some(ds.blks_hit.load(Relaxed).to_string()),
            Some(ds.tup_returned.load(Relaxed).to_string()),
            Some(ds.tup_fetched.load(Relaxed).to_string()),
            Some(ds.tup_inserted.load(Relaxed).to_string()),
            Some(ds.tup_updated.load(Relaxed).to_string()),
            Some(ds.tup_deleted.load(Relaxed).to_string()),
            Some(ds.temp_files.load(Relaxed).to_string()),
            Some(ds.temp_bytes.load(Relaxed).to_string()),
            Some(ds.deadlocks.load(Relaxed).to_string()),
            Some(format!("{:.3}", ds.blk_read_time_us.load(Relaxed) as f64 / 1000.0)),
            Some(format!("{:.3}", ds.blk_write_time_us.load(Relaxed) as f64 / 1000.0)),
            Some(format!("{:.3}", uptime_ms)),
            Some(format!("{:.3}", active_ms)),
            Some(ds.sessions.load(Relaxed).to_string()),
            None, // stats_reset
        ]];
        self.single_row_result(cols, rows)
    }

    /// pg_stat_bgwriter — background writer / checkpoint stats.
    fn handle_pg_stat_bgwriter(&self) -> Vec<BackendMessage> {
        let ws = self.storage.wal_stats_snapshot();
        let cols = vec![
            ("checkpoints_timed", 20, 8i16),
            ("checkpoints_req", 20, 8),
            ("checkpoint_write_time", 701, 8),
            ("checkpoint_sync_time", 701, 8),
            ("buffers_checkpoint", 20, 8),
            ("buffers_clean", 20, 8),
            ("maxwritten_clean", 20, 8),
            ("buffers_backend", 20, 8),
            ("buffers_backend_fsync", 20, 8),
            ("buffers_alloc", 20, 8),
            ("stats_reset", 1184, 8),
        ];
        let rows = vec![vec![
            Some("0".into()),
            Some("0".into()),
            Some("0".into()),
            Some(format!("{:.3}", ws.fsync_total_us as f64 / 1000.0)),
            Some("0".into()),
            Some("0".into()),
            Some("0".into()),
            Some(ws.flushes.to_string()),
            Some("0".into()),
            Some("0".into()),
            None,
        ]];
        self.single_row_result(cols, rows)
    }

    fn handle_information_schema_referential_constraints(&self) -> Vec<BackendMessage> {
        let catalog = self.storage.get_catalog();
        let tables = catalog.list_tables();
        let cols = vec![
            ("constraint_catalog", 25, -1i16),
            ("constraint_schema", 25, -1),
            ("constraint_name", 25, -1),
            ("unique_constraint_catalog", 25, -1),
            ("unique_constraint_schema", 25, -1),
            ("unique_constraint_name", 25, -1),
            ("match_option", 25, -1),
            ("update_rule", 25, -1),
            ("delete_rule", 25, -1),
        ];
        let mut rows = Vec::new();
        for t in &tables {
            for (i, fk) in t.foreign_keys.iter().enumerate() {
                rows.push(vec![
                    Some("falcon".into()),
                    Some("public".into()),
                    Some(format!("{}_fkey_{}", t.name, i)),
                    Some("falcon".into()),
                    Some("public".into()),
                    Some(format!("{}_pkey", fk.ref_table)),
                    Some("NONE".into()),
                    Some("NO ACTION".into()),
                    Some("NO ACTION".into()),
                ]);
            }
        }
        self.single_row_result(cols, rows)
    }

    fn handle_information_schema_constraint_column_usage(&self) -> Vec<BackendMessage> {
        let catalog = self.storage.get_catalog();
        let tables = catalog.list_tables();
        let cols = vec![
            ("table_catalog", 25, -1i16),
            ("table_schema", 25, -1),
            ("table_name", 25, -1),
            ("column_name", 25, -1),
            ("constraint_catalog", 25, -1),
            ("constraint_schema", 25, -1),
            ("constraint_name", 25, -1),
        ];
        let mut rows = Vec::new();
        for t in &tables {
            for &col_idx in &t.primary_key_columns {
                rows.push(vec![
                    Some("falcon".into()),
                    Some("public".into()),
                    Some(t.name.clone()),
                    Some(t.columns[col_idx].name.clone()),
                    Some("falcon".into()),
                    Some("public".into()),
                    Some(format!("{}_pkey", t.name)),
                ]);
            }
        }
        self.single_row_result(cols, rows)
    }
}
