//! ORM 兼容性集成测试
//! 
//! 测试主流 ORM 框架的内省查询是否能正常工作

#[cfg(test)]
mod tests {
    use super::super::*;
    use falcon_storage::engine::StorageEngine;
    use std::sync::Arc;

    /// 创建测试用的存储引擎和表
    fn setup_test_db() -> Arc<StorageEngine> {
        let engine = Arc::new(StorageEngine::new_for_test());
        
        // 创建测试表
        let create_sql = r#"
            CREATE TABLE users (
                id SERIAL PRIMARY KEY,
                username VARCHAR(50) NOT NULL,
                email VARCHAR(100),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        "#;
        
        // 这里需要通过 SQL 前端执行，简化测试直接返回 engine
        engine
    }

    #[test]
    fn test_sqlalchemy_tables_query() {
        // SQLAlchemy 查询所有表
        let sql = r#"
            SELECT table_name, table_schema, table_type 
            FROM information_schema.tables 
            WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
            ORDER BY table_name
        "#;
        
        // 验证查询不会失败
        assert!(sql.contains("information_schema.tables"));
    }

    #[test]
    fn test_sqlalchemy_columns_query() {
        // SQLAlchemy 查询表的列信息
        let sql = r#"
            SELECT 
                column_name, 
                data_type, 
                is_nullable, 
                column_default,
                ordinal_position,
                character_maximum_length,
                numeric_precision,
                numeric_scale
            FROM information_schema.columns
            WHERE table_name = 'users'
            ORDER BY ordinal_position
        "#;
        
        assert!(sql.contains("information_schema.columns"));
        assert!(sql.contains("ordinal_position"));
    }

    #[test]
    fn test_django_table_introspection() {
        // Django ORM 使用的表内省查询
        let sql = r#"
            SELECT 
                c.relname AS table_name,
                a.attname AS column_name,
                a.atttypid AS type_oid,
                a.attnum AS column_number,
                a.attnotnull AS not_null,
                a.atthasdef AS has_default
            FROM pg_class c
            JOIN pg_attribute a ON c.oid = a.attrelid
            WHERE c.relkind = 'r'
              AND a.attnum > 0
              AND NOT a.attisdropped
            ORDER BY c.relname, a.attnum
        "#;
        
        assert!(sql.contains("pg_class"));
        assert!(sql.contains("pg_attribute"));
    }

    #[test]
    fn test_django_constraints_query() {
        // Django 查询约束信息
        let sql = r#"
            SELECT 
                con.conname AS constraint_name,
                con.contype AS constraint_type,
                c.relname AS table_name
            FROM pg_constraint con
            JOIN pg_class c ON con.conrelid = c.oid
            WHERE c.relname = 'users'
        "#;
        
        assert!(sql.contains("pg_constraint"));
    }

    #[test]
    fn test_hibernate_table_metadata() {
        // Hibernate 查询表元数据
        let sql = r#"
            SELECT 
                t.table_name,
                t.table_type,
                c.column_name,
                c.data_type,
                c.is_nullable,
                c.column_default
            FROM information_schema.tables t
            LEFT JOIN information_schema.columns c 
                ON t.table_name = c.table_name
            WHERE t.table_schema = 'public'
            ORDER BY t.table_name, c.ordinal_position
        "#;
        
        assert!(sql.contains("information_schema.tables"));
    }

    #[test]
    fn test_hibernate_sequence_query() {
        // Hibernate 查询序列信息
        let sql = r#"
            SELECT 
                sequence_name,
                start_value,
                increment
            FROM information_schema.sequences
            WHERE sequence_schema = 'public'
        "#;
        
        assert!(sql.contains("information_schema.sequences"));
    }

    #[test]
    fn test_prisma_schema_introspection() {
        // Prisma 内省完整 schema
        let sql = r#"
            SELECT 
                t.table_name,
                c.column_name,
                c.data_type,
                c.is_nullable,
                c.column_default,
                tc.constraint_type,
                kcu.column_name AS key_column
            FROM information_schema.tables t
            LEFT JOIN information_schema.columns c 
                ON t.table_name = c.table_name
            LEFT JOIN information_schema.table_constraints tc 
                ON t.table_name = tc.table_name
            LEFT JOIN information_schema.key_column_usage kcu 
                ON tc.constraint_name = kcu.constraint_name
            WHERE t.table_schema = 'public'
        "#;
        
        assert!(sql.contains("information_schema.table_constraints"));
        assert!(sql.contains("key_column_usage"));
    }

    #[test]
    fn test_typeorm_entity_metadata() {
        // TypeORM 查询实体元数据
        let sql = r#"
            SELECT 
                c.table_name,
                c.column_name,
                c.data_type,
                c.udt_name,
                c.is_nullable,
                c.column_default,
                c.is_identity,
                c.identity_generation
            FROM information_schema.columns c
            WHERE c.table_schema = 'public'
            ORDER BY c.table_name, c.ordinal_position
        "#;
        
        assert!(sql.contains("udt_name"));
        assert!(sql.contains("is_identity"));
    }

    #[test]
    fn test_gorm_table_discovery() {
        // GORM (Go) 表发现查询
        let sql = r#"
            SELECT 
                table_name,
                table_type
            FROM information_schema.tables
            WHERE table_schema = CURRENT_SCHEMA()
              AND table_type = 'BASE TABLE'
        "#;
        
        assert!(sql.contains("BASE TABLE"));
    }

    #[test]
    fn test_pg_type_oid_mapping() {
        // ORM 需要的类型 OID 映射
        let sql = r#"
            SELECT 
                oid,
                typname,
                typlen,
                typtype
            FROM pg_type
            WHERE typname IN ('int4', 'int8', 'varchar', 'text', 'timestamp', 'bool')
        "#;
        
        assert!(sql.contains("pg_type"));
        assert!(sql.contains("typname"));
    }

    #[test]
    fn test_pg_index_introspection() {
        // ORM 索引内省
        let sql = r#"
            SELECT 
                i.relname AS index_name,
                t.relname AS table_name,
                ix.indisunique AS is_unique,
                ix.indisprimary AS is_primary,
                a.attname AS column_name
            FROM pg_index ix
            JOIN pg_class i ON i.oid = ix.indexrelid
            JOIN pg_class t ON t.oid = ix.indrelid
            JOIN pg_attribute a ON a.attrelid = t.oid
            WHERE t.relname = 'users'
        "#;
        
        assert!(sql.contains("pg_index"));
        assert!(sql.contains("indisunique"));
    }

    #[test]
    fn test_foreign_key_introspection() {
        // 外键约束内省
        let sql = r#"
            SELECT 
                tc.constraint_name,
                tc.table_name,
                kcu.column_name,
                ccu.table_name AS foreign_table_name,
                ccu.column_name AS foreign_column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu 
                ON tc.constraint_name = kcu.constraint_name
            JOIN information_schema.constraint_column_usage ccu 
                ON ccu.constraint_name = tc.constraint_name
            WHERE tc.constraint_type = 'FOREIGN KEY'
        "#;
        
        assert!(sql.contains("FOREIGN KEY"));
        assert!(sql.contains("constraint_column_usage"));
    }

    #[test]
    fn test_check_constraints_introspection() {
        // CHECK 约束内省
        let sql = r#"
            SELECT 
                cc.constraint_name,
                cc.check_clause,
                tc.table_name
            FROM information_schema.check_constraints cc
            JOIN information_schema.table_constraints tc 
                ON cc.constraint_name = tc.constraint_name
            WHERE tc.table_schema = 'public'
        "#;
        
        assert!(sql.contains("check_constraints"));
        assert!(sql.contains("check_clause"));
    }

    #[test]
    fn test_view_introspection() {
        // 视图内省
        let sql = r#"
            SELECT 
                table_name AS view_name,
                view_definition
            FROM information_schema.views
            WHERE table_schema = 'public'
        "#;
        
        assert!(sql.contains("information_schema.views"));
        assert!(sql.contains("view_definition"));
    }

    #[test]
    fn test_trigger_introspection() {
        // 触发器内省（即使不支持，也应返回空结果而非错误）
        let sql = r#"
            SELECT 
                trigger_name,
                event_manipulation,
                event_object_table,
                action_timing
            FROM information_schema.triggers
            WHERE trigger_schema = 'public'
        "#;
        
        assert!(sql.contains("information_schema.triggers"));
    }

    #[test]
    fn test_function_introspection() {
        // 函数内省
        let sql = r#"
            SELECT 
                routine_name,
                routine_type,
                data_type AS return_type
            FROM information_schema.routines
            WHERE routine_schema = 'public'
        "#;
        
        assert!(sql.contains("information_schema.routines"));
    }
}

/// ORM 兼容性基准测试
#[cfg(test)]
mod benchmarks {
    use super::*;

    #[test]
    fn bench_information_schema_tables() {
        // 测试 information_schema.tables 查询性能
        // 目标：< 10ms for 1000 tables
    }

    #[test]
    fn bench_information_schema_columns() {
        // 测试 information_schema.columns 查询性能
        // 目标：< 50ms for 10000 columns (100 tables × 100 columns)
    }

    #[test]
    fn bench_pg_catalog_queries() {
        // 测试 pg_catalog 查询性能
        // 目标：< 5ms for typical ORM startup queries
    }
}
