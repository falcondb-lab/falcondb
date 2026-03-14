//! 扩展的 pg_catalog 系统表处理器
//! 
//! 为 ORM 框架提供完整的 PostgreSQL 系统目录兼容性

use crate::codec::{BackendMessage, FieldDescription};
use crate::handler::QueryHandler;
use falcon_common::types::TableId;
use std::sync::Arc;

impl QueryHandler {
    /// 处理 pg_proc 查询（函数/过程列表）
    /// 
    /// ORM 使用场景：
    /// - SQLAlchemy: 检测可用的内置函数
    /// - Hibernate: 验证存储过程
    /// - Prisma: 内省数据库函数
    pub(crate) fn handle_pg_proc_extended(&self, sql_lower: &str) -> Vec<BackendMessage> {
        // 返回内置函数列表
        let builtin_functions = vec![
            // 字符串函数
            ("1000", "length", "11", "10", "12", "100.0", "0.0", "0", "0", "f", "f", "f", "t", "f", "i", "s", "1", "0", "23", "25", "", "", "", "", "", "length", "", "", ""),
            ("1001", "upper", "11", "10", "12", "100.0", "0.0", "0", "0", "f", "f", "f", "t", "f", "i", "s", "1", "0", "25", "25", "", "", "", "", "", "upper", "", "", ""),
            ("1002", "lower", "11", "10", "12", "100.0", "0.0", "0", "0", "f", "f", "f", "t", "f", "i", "s", "1", "0", "25", "25", "", "", "", "", "", "lower", "", "", ""),
            
            // 数学函数
            ("1100", "abs", "11", "10", "12", "100.0", "0.0", "0", "0", "f", "f", "f", "t", "f", "i", "s", "1", "0", "23", "23", "", "", "", "", "", "abs", "", "", ""),
            ("1101", "ceil", "11", "10", "12", "100.0", "0.0", "0", "0", "f", "f", "f", "t", "f", "i", "s", "1", "0", "701", "701", "", "", "", "", "", "ceil", "", "", ""),
            ("1102", "floor", "11", "10", "12", "100.0", "0.0", "0", "0", "f", "f", "f", "t", "f", "i", "s", "1", "0", "701", "701", "", "", "", "", "", "floor", "", "", ""),
            
            // 聚合函数
            ("2100", "count", "11", "10", "12", "100.0", "0.0", "0", "0", "a", "f", "f", "f", "f", "i", "s", "1", "0", "20", "2276", "", "", "", "", "", "aggregate_dummy", "", "", ""),
            ("2101", "sum", "11", "10", "12", "100.0", "0.0", "0", "0", "a", "f", "f", "f", "f", "i", "s", "1", "0", "1700", "1700", "", "", "", "", "", "aggregate_dummy", "", "", ""),
            ("2102", "avg", "11", "10", "12", "100.0", "0.0", "0", "0", "a", "f", "f", "f", "f", "i", "s", "1", "0", "1700", "1700", "", "", "", "", "", "aggregate_dummy", "", "", ""),
            ("2103", "min", "11", "10", "12", "100.0", "0.0", "0", "0", "a", "f", "f", "f", "f", "i", "s", "1", "0", "2276", "2276", "", "", "", "", "", "aggregate_dummy", "", "", ""),
            ("2104", "max", "11", "10", "12", "100.0", "0.0", "0", "0", "a", "f", "f", "f", "f", "i", "s", "1", "0", "2276", "2276", "", "", "", "", "", "aggregate_dummy", "", "", ""),
            
            // 日期时间函数
            ("3000", "now", "11", "10", "12", "100.0", "0.0", "0", "0", "f", "f", "f", "f", "f", "s", "s", "0", "0", "1184", "", "", "", "", "", "", "now", "", "", ""),
            ("3001", "current_timestamp", "11", "10", "12", "100.0", "0.0", "0", "0", "f", "f", "f", "f", "f", "s", "s", "0", "0", "1184", "", "", "", "", "", "", "current_timestamp", "", "", ""),
            ("3002", "current_date", "11", "10", "12", "100.0", "0.0", "0", "0", "f", "f", "f", "f", "f", "s", "s", "0", "0", "1082", "", "", "", "", "", "", "current_date", "", "", ""),
            
            // JSON 函数
            ("4000", "jsonb_extract_path", "11", "10", "12", "100.0", "0.0", "0", "0", "f", "f", "f", "f", "f", "i", "s", "2", "0", "3802", "3802 1009", "", "", "", "", "", "jsonb_extract_path", "", "", ""),
            ("4001", "jsonb_typeof", "11", "10", "12", "100.0", "0.0", "0", "0", "f", "f", "f", "t", "f", "i", "s", "1", "0", "25", "3802", "", "", "", "", "", "jsonb_typeof", "", "", ""),
        ];

        let schema = vec![
            ("oid", 26, 4i16),
            ("proname", 25, -1),
            ("pronamespace", 26, 4),
            ("proowner", 26, 4),
            ("prolang", 26, 4),
            ("procost", 701, 8),
            ("prorows", 701, 8),
            ("provariadic", 26, 4),
            ("prosupport", 26, 4),
            ("prokind", 18, 1),
            ("prosecdef", 16, 1),
            ("proleakproof", 16, 1),
            ("proisstrict", 16, 1),
            ("proretset", 16, 1),
            ("provolatile", 18, 1),
            ("proparallel", 18, 1),
            ("pronargs", 21, 2),
            ("pronargdefaults", 21, 2),
            ("prorettype", 26, 4),
            ("proargtypes", 30, -1),
            ("proallargtypes", 1028, -1),
            ("proargmodes", 1002, -1),
            ("proargnames", 1009, -1),
            ("proargdefaults", 25, -1),
            ("protrftypes", 1028, -1),
            ("prosrc", 25, -1),
            ("probin", 25, -1),
            ("proconfig", 1009, -1),
            ("proacl", 1034, -1),
        ];

        let rows: Vec<Vec<Option<String>>> = builtin_functions
            .iter()
            .map(|f| {
                vec![
                    Some(f.0.to_string()),   // oid
                    Some(f.1.to_string()),   // proname
                    Some(f.2.to_string()),   // pronamespace
                    Some(f.3.to_string()),   // proowner
                    Some(f.4.to_string()),   // prolang
                    Some(f.5.to_string()),   // procost
                    Some(f.6.to_string()),   // prorows
                    Some(f.7.to_string()),   // provariadic
                    Some(f.8.to_string()),   // prosupport
                    Some(f.9.to_string()),   // prokind
                    Some(f.10.to_string()),  // prosecdef
                    Some(f.11.to_string()),  // proleakproof
                    Some(f.12.to_string()),  // proisstrict
                    Some(f.13.to_string()),  // proretset
                    Some(f.14.to_string()),  // provolatile
                    Some(f.15.to_string()),  // proparallel
                    Some(f.16.to_string()),  // pronargs
                    Some(f.17.to_string()),  // pronargdefaults
                    Some(f.18.to_string()),  // prorettype
                    Some(f.19.to_string()),  // proargtypes
                    Some(f.20.to_string()),  // proallargtypes
                    Some(f.21.to_string()),  // proargmodes
                    Some(f.22.to_string()),  // proargnames
                    Some(f.23.to_string()),  // proargdefaults
                    Some(f.24.to_string()),  // protrftypes
                    Some(f.25.to_string()),  // prosrc
                    Some(f.26.to_string()),  // probin
                    Some(f.27.to_string()),  // proconfig
                    Some(f.28.to_string()),  // proacl
                ]
            })
            .collect();

        self.single_row_result(schema, rows)
    }

    /// 处理 pg_trigger 查询（触发器列表）
    /// 
    /// ORM 使用场景：
    /// - Django: 检测表上的触发器
    /// - SQLAlchemy: 内省触发器定义
    pub(crate) fn handle_pg_trigger(&self, sql_lower: &str) -> Vec<BackendMessage> {
        // FalconDB 当前不支持触发器，返回空结果
        let schema = vec![
            ("oid", 26, 4i16),
            ("tgrelid", 26, 4),
            ("tgparentid", 26, 4),
            ("tgname", 25, -1),
            ("tgfoid", 26, 4),
            ("tgtype", 21, 2),
            ("tgenabled", 18, 1),
            ("tgisinternal", 16, 1),
            ("tgconstrrelid", 26, 4),
            ("tgconstrindid", 26, 4),
            ("tgconstraint", 26, 4),
            ("tgdeferrable", 16, 1),
            ("tginitdeferred", 16, 1),
            ("tgnargs", 21, 2),
            ("tgattr", 22, -1),
            ("tgargs", 17, -1),
            ("tgqual", 25, -1),
            ("tgoldtable", 25, -1),
            ("tgnewtable", 25, -1),
        ];

        self.single_row_result(schema, vec![])
    }

    /// 处理 pg_aggregate 查询（聚合函数定义）
    pub(crate) fn handle_pg_aggregate(&self, sql_lower: &str) -> Vec<BackendMessage> {
        let aggregates = vec![
            ("2100", "n", "0", "0", "0", "0", "0", "0", "0", "0", "0", "f", "f", "r", "r", "0", "20", "0", "0", "0", "", ""),
            ("2101", "n", "0", "0", "0", "0", "0", "0", "0", "0", "0", "f", "f", "r", "r", "0", "1700", "0", "0", "0", "", ""),
            ("2102", "n", "0", "0", "0", "0", "0", "0", "0", "0", "0", "f", "f", "r", "r", "0", "1700", "0", "0", "0", "", ""),
        ];

        let schema = vec![
            ("aggfnoid", 26, 4i16),
            ("aggkind", 18, 1),
            ("aggnumdirectargs", 21, 2),
            ("aggtransfn", 26, 4),
            ("aggfinalfn", 26, 4),
            ("aggcombinefn", 26, 4),
            ("aggserialfn", 26, 4),
            ("aggdeserialfn", 26, 4),
            ("aggmtransfn", 26, 4),
            ("aggminvtransfn", 26, 4),
            ("aggmfinalfn", 26, 4),
            ("aggfinalextra", 16, 1),
            ("aggmfinalextra", 16, 1),
            ("aggfinalmodify", 18, 1),
            ("aggmfinalmodify", 18, 1),
            ("aggsortop", 26, 4),
            ("aggtranstype", 26, 4),
            ("aggtransspace", 23, 4),
            ("aggmtranstype", 26, 4),
            ("aggmtransspace", 23, 4),
            ("agginitval", 25, -1),
            ("aggminitval", 25, -1),
        ];

        let rows: Vec<Vec<Option<String>>> = aggregates
            .iter()
            .map(|a| {
                vec![
                    Some(a.0.to_string()),
                    Some(a.1.to_string()),
                    Some(a.2.to_string()),
                    Some(a.3.to_string()),
                    Some(a.4.to_string()),
                    Some(a.5.to_string()),
                    Some(a.6.to_string()),
                    Some(a.7.to_string()),
                    Some(a.8.to_string()),
                    Some(a.9.to_string()),
                    Some(a.10.to_string()),
                    Some(a.11.to_string()),
                    Some(a.12.to_string()),
                    Some(a.13.to_string()),
                    Some(a.14.to_string()),
                    Some(a.15.to_string()),
                    Some(a.16.to_string()),
                    Some(a.17.to_string()),
                    Some(a.18.to_string()),
                    Some(a.19.to_string()),
                    Some(a.20.to_string()),
                    Some(a.21.to_string()),
                ]
            })
            .collect();

        self.single_row_result(schema, rows)
    }

    /// 处理 pg_language 查询（编程语言列表）
    pub(crate) fn handle_pg_language(&self) -> Vec<BackendMessage> {
        let languages = vec![
            ("12", "internal", "10", "t", "f", "f", "0", "0", "0", "0", ""),
            ("13", "c", "10", "f", "f", "f", "0", "0", "0", "0", ""),
            ("14", "sql", "10", "t", "t", "f", "0", "0", "0", "0", ""),
            ("2561", "plpgsql", "10", "t", "t", "t", "2560", "2559", "2558", "0", ""),
        ];

        let schema = vec![
            ("oid", 26, 4i16),
            ("lanname", 25, -1),
            ("lanowner", 26, 4),
            ("lanispl", 16, 1),
            ("lanpltrusted", 16, 1),
            ("lanplcallfoid", 26, 4),
            ("laninline", 26, 4),
            ("lanvalidator", 26, 4),
            ("lanacl", 1034, -1),
        ];

        let rows: Vec<Vec<Option<String>>> = languages
            .iter()
            .map(|l| {
                vec![
                    Some(l.0.to_string()),
                    Some(l.1.to_string()),
                    Some(l.2.to_string()),
                    Some(l.3.to_string()),
                    Some(l.4.to_string()),
                    Some(l.5.to_string()),
                    Some(l.6.to_string()),
                    Some(l.7.to_string()),
                    Some(l.8.to_string()),
                ]
            })
            .collect();

        self.single_row_result(schema, rows)
    }

    /// 处理 pg_opclass 查询（操作符类）
    pub(crate) fn handle_pg_opclass(&self) -> Vec<BackendMessage> {
        let opclasses = vec![
            ("421", "403", "abstime_ops", "11", "10", "702", "t"),
            ("397", "403", "array_ops", "11", "10", "2277", "t"),
            ("423", "403", "bit_ops", "11", "10", "1560", "t"),
            ("424", "403", "bool_ops", "11", "10", "16", "t"),
            ("426", "403", "bpchar_ops", "11", "10", "1042", "t"),
            ("427", "403", "bytea_ops", "11", "10", "17", "t"),
            ("1181", "403", "date_ops", "11", "10", "1082", "t"),
            ("1970", "403", "float4_ops", "11", "10", "700", "t"),
            ("1971", "403", "float8_ops", "11", "10", "701", "t"),
            ("1974", "403", "int2_ops", "11", "10", "21", "t"),
            ("1976", "403", "int4_ops", "11", "10", "23", "t"),
            ("1978", "403", "int8_ops", "11", "10", "20", "t"),
            ("1981", "403", "numeric_ops", "11", "10", "1700", "t"),
            ("1994", "403", "text_ops", "11", "10", "25", "t"),
            ("1996", "403", "time_ops", "11", "10", "1083", "t"),
            ("2040", "403", "timestamp_ops", "11", "10", "1114", "t"),
            ("2542", "403", "timestamptz_ops", "11", "10", "1184", "t"),
            ("3626", "403", "uuid_ops", "11", "10", "2950", "t"),
            ("3802", "403", "jsonb_ops", "11", "10", "3802", "t"),
        ];

        let schema = vec![
            ("oid", 26, 4i16),
            ("opcmethod", 26, 4),
            ("opcname", 25, -1),
            ("opcnamespace", 26, 4),
            ("opcowner", 26, 4),
            ("opcintype", 26, 4),
            ("opcdefault", 16, 1),
        ];

        let rows: Vec<Vec<Option<String>>> = opclasses
            .iter()
            .map(|o| {
                vec![
                    Some(o.0.to_string()),
                    Some(o.1.to_string()),
                    Some(o.2.to_string()),
                    Some(o.3.to_string()),
                    Some(o.4.to_string()),
                    Some(o.5.to_string()),
                    Some(o.6.to_string()),
                ]
            })
            .collect();

        self.single_row_result(schema, rows)
    }
}
