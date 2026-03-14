//! ORM Compatibility Layer
//!
//! 增强 pg_catalog 和 information_schema 支持，确保主流 ORM 框架的内省查询正常工作。
//! 
//! 支持的 ORM：
//! - SQLAlchemy (Python)
//! - Django ORM (Python)
//! - Hibernate (Java)
//! - Prisma (Node.js/TypeScript)
//! - TypeORM (Node.js/TypeScript)
//! - GORM (Go)

use std::sync::Arc;
use falcon_common::schema::TableSchema;
use falcon_common::types::{DataType, TableId};
use falcon_storage::engine::StorageEngine;

/// ORM 特定查询模式识别和优化
pub struct OrmQueryOptimizer {
    engine: Arc<StorageEngine>,
}

impl OrmQueryOptimizer {
    pub fn new(engine: Arc<StorageEngine>) -> Self {
        Self { engine }
    }

    /// 检测并优化 SQLAlchemy 内省查询
    pub fn optimize_sqlalchemy_query(&self, sql: &str) -> Option<String> {
        // SQLAlchemy 常见查询模式
        if sql.contains("information_schema.tables") 
            && sql.contains("table_schema") 
            && sql.contains("table_type") {
            return Some(self.build_sqlalchemy_tables_query());
        }

        if sql.contains("information_schema.columns") 
            && sql.contains("ordinal_position") {
            return Some(self.build_sqlalchemy_columns_query());
        }

        None
    }

    /// 检测并优化 Django ORM 内省查询
    pub fn optimize_django_query(&self, sql: &str) -> Option<String> {
        // Django 使用 pg_catalog.pg_class 和 pg_attribute
        if sql.contains("pg_class c") 
            && sql.contains("pg_attribute a") 
            && sql.contains("attnum > 0") {
            return Some(self.build_django_table_info_query());
        }

        None
    }

    /// 检测并优化 Prisma 内省查询
    pub fn optimize_prisma_query(&self, sql: &str) -> Option<String> {
        // Prisma 查询 information_schema 和 pg_constraint
        if sql.contains("information_schema.table_constraints")
            && sql.contains("constraint_type") {
            return Some(self.build_prisma_constraints_query());
        }

        None
    }

    fn build_sqlalchemy_tables_query(&self) -> String {
        // 返回优化的表列表查询
        "SELECT table_name, table_schema, table_type FROM information_schema.tables WHERE table_schema NOT IN ('pg_catalog', 'information_schema')".to_string()
    }

    fn build_sqlalchemy_columns_query(&self) -> String {
        "SELECT column_name, data_type, is_nullable, column_default, ordinal_position FROM information_schema.columns ORDER BY ordinal_position".to_string()
    }

    fn build_django_table_info_query(&self) -> String {
        "SELECT c.relname, a.attname, a.atttypid, a.attnum FROM pg_class c JOIN pg_attribute a ON c.oid = a.attrelid WHERE a.attnum > 0 AND NOT a.attisdropped".to_string()
    }

    fn build_prisma_constraints_query(&self) -> String {
        "SELECT constraint_name, constraint_type, table_name FROM information_schema.table_constraints".to_string()
    }
}

/// 扩展的 pg_catalog 表定义
pub struct ExtendedPgCatalog;

impl ExtendedPgCatalog {
    /// pg_proc - 函数/过程定义（ORM 内省需要）
    pub fn pg_proc_schema() -> Vec<(&'static str, i32, i16)> {
        vec![
            ("oid", 26, 4),              // OID
            ("proname", 25, -1),         // 函数名
            ("pronamespace", 26, 4),     // 命名空间 OID
            ("proowner", 26, 4),         // 所有者 OID
            ("prolang", 26, 4),          // 语言 OID
            ("procost", 701, 8),         // 估计执行成本
            ("prorows", 701, 8),         // 估计返回行数
            ("provariadic", 26, 4),      // 可变参数类型
            ("prosupport", 26, 4),       // 支持函数
            ("prokind", 18, 1),          // f=function, p=procedure, a=aggregate, w=window
            ("prosecdef", 16, 1),        // 安全定义
            ("proleakproof", 16, 1),     // 泄漏证明
            ("proisstrict", 16, 1),      // 严格模式（NULL 输入返回 NULL）
            ("proretset", 16, 1),        // 返回集合
            ("provolatile", 18, 1),      // i=immutable, s=stable, v=volatile
            ("proparallel", 18, 1),      // s=safe, r=restricted, u=unsafe
            ("pronargs", 21, 2),         // 参数数量
            ("pronargdefaults", 21, 2),  // 有默认值的参数数量
            ("prorettype", 26, 4),       // 返回类型 OID
            ("proargtypes", 30, -1),     // 参数类型数组
            ("proallargtypes", 1028, -1), // 所有参数类型（包括 OUT）
            ("proargmodes", 1002, -1),   // 参数模式（i/o/b/v/t）
            ("proargnames", 1009, -1),   // 参数名称数组
            ("proargdefaults", 25, -1),  // 默认值表达式
            ("protrftypes", 1028, -1),   // 转换类型
            ("prosrc", 25, -1),          // 函数源代码
            ("probin", 25, -1),          // 动态库路径
            ("proconfig", 1009, -1),     // 配置参数
            ("proacl", 1034, -1),        // 访问权限
        ]
    }

    /// pg_depend - 对象依赖关系（pg_dump 和 ORM 迁移工具需要）
    pub fn pg_depend_schema() -> Vec<(&'static str, i32, i16)> {
        vec![
            ("classid", 26, 4),      // 依赖对象的系统目录 OID
            ("objid", 26, 4),        // 依赖对象的 OID
            ("objsubid", 23, 4),     // 子对象 ID（列号）
            ("refclassid", 26, 4),   // 被依赖对象的系统目录 OID
            ("refobjid", 26, 4),     // 被依赖对象的 OID
            ("refobjsubid", 23, 4),  // 被依赖子对象 ID
            ("deptype", 18, 1),      // 依赖类型：n=normal, a=auto, i=internal, e=extension, p=pin
        ]
    }

    /// pg_trigger - 触发器定义（ORM 内省）
    pub fn pg_trigger_schema() -> Vec<(&'static str, i32, i16)> {
        vec![
            ("oid", 26, 4),
            ("tgrelid", 26, 4),          // 表 OID
            ("tgparentid", 26, 4),       // 父触发器 OID
            ("tgname", 25, -1),          // 触发器名称
            ("tgfoid", 26, 4),           // 触发器函数 OID
            ("tgtype", 21, 2),           // 触发器类型位掩码
            ("tgenabled", 18, 1),        // O=enabled, D=disabled, R=replica, A=always
            ("tgisinternal", 16, 1),     // 内部触发器
            ("tgconstrrelid", 26, 4),    // 约束表 OID
            ("tgconstrindid", 26, 4),    // 约束索引 OID
            ("tgconstraint", 26, 4),     // 约束 OID
            ("tgdeferrable", 16, 1),     // 可延迟
            ("tginitdeferred", 16, 1),   // 初始延迟
            ("tgnargs", 21, 2),          // 参数数量
            ("tgattr", 22, -1),          // 列号数组
            ("tgargs", 17, -1),          // 参数字节数组
            ("tgqual", 25, -1),          // WHEN 条件
            ("tgoldtable", 25, -1),      // OLD TABLE 名称
            ("tgnewtable", 25, -1),      // NEW TABLE 名称
        ]
    }

    /// pg_enum - 枚举类型值（ORM 类型映射）
    pub fn pg_enum_schema() -> Vec<(&'static str, i32, i16)> {
        vec![
            ("oid", 26, 4),
            ("enumtypid", 26, 4),        // 枚举类型 OID
            ("enumsortorder", 701, 8),   // 排序顺序
            ("enumlabel", 25, -1),       // 枚举标签
        ]
    }

    /// pg_aggregate - 聚合函数定义
    pub fn pg_aggregate_schema() -> Vec<(&'static str, i32, i16)> {
        vec![
            ("aggfnoid", 26, 4),         // 聚合函数 OID
            ("aggkind", 18, 1),          // n=normal, o=ordered-set, h=hypothetical-set
            ("aggnumdirectargs", 21, 2), // 直接参数数量
            ("aggtransfn", 26, 4),       // 转换函数
            ("aggfinalfn", 26, 4),       // 最终函数
            ("aggcombinefn", 26, 4),     // 组合函数
            ("aggserialfn", 26, 4),      // 序列化函数
            ("aggdeserialfn", 26, 4),    // 反序列化函数
            ("aggmtransfn", 26, 4),      // 移动聚合转换函数
            ("aggminvtransfn", 26, 4),   // 移动聚合逆转换函数
            ("aggmfinalfn", 26, 4),      // 移动聚合最终函数
            ("aggfinalextra", 16, 1),    // 最终函数需要额外参数
            ("aggmfinalextra", 16, 1),   // 移动最终函数需要额外参数
            ("aggfinalmodify", 18, 1),   // r=read-only, s=shareable, w=read-write
            ("aggmfinalmodify", 18, 1),  // 移动最终函数修改模式
            ("aggsortop", 26, 4),        // 排序操作符
            ("aggtranstype", 26, 4),     // 转换状态类型
            ("aggtransspace", 23, 4),    // 转换状态空间估计
            ("aggmtranstype", 26, 4),    // 移动聚合转换类型
            ("aggmtransspace", 23, 4),   // 移动转换空间
            ("agginitval", 25, -1),      // 初始值
            ("aggminitval", 25, -1),     // 移动聚合初始值
        ]
    }
}

/// ORM 兼容性测试辅助函数
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sqlalchemy_pattern_detection() {
        let optimizer = OrmQueryOptimizer::new(Arc::new(StorageEngine::new_for_test()));
        
        let sql = "SELECT table_name, table_schema, table_type FROM information_schema.tables WHERE table_schema = 'public'";
        assert!(optimizer.optimize_sqlalchemy_query(sql).is_some());
    }

    #[test]
    fn test_django_pattern_detection() {
        let optimizer = OrmQueryOptimizer::new(Arc::new(StorageEngine::new_for_test()));
        
        let sql = "SELECT c.relname, a.attname FROM pg_class c JOIN pg_attribute a ON c.oid = a.attrelid WHERE a.attnum > 0";
        assert!(optimizer.optimize_django_query(sql).is_some());
    }

    #[test]
    fn test_pg_proc_schema() {
        let schema = ExtendedPgCatalog::pg_proc_schema();
        assert_eq!(schema.len(), 29);
        assert_eq!(schema[0].0, "oid");
        assert_eq!(schema[1].0, "proname");
    }
}
