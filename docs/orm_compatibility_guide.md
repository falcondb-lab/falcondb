# FalconDB ORM 兼容性指南

## 概述

FalconDB 提供了增强的 PostgreSQL 系统目录（`pg_catalog`）和信息模式（`information_schema`）支持，确保主流 ORM 框架能够正常进行 schema 内省和迁移操作。

## 支持的 ORM 框架

### ✅ 完全兼容

| ORM | 语言 | 版本 | 状态 | 说明 |
|-----|------|------|------|------|
| **psycopg2/psycopg3** | Python | 2.9+/3.0+ | ✅ 完全兼容 | 标准 PostgreSQL 驱动 |
| **pgx** | Go | 5.x | ✅ 完全兼容 | 高性能 Go 驱动 |
| **pgjdbc** | Java | 42.7+ | ✅ 完全兼容 | JDBC 标准驱动 |
| **node-postgres** | Node.js | 8.x | ✅ 完全兼容 | Node.js 标准驱动 |

### ⚠️ 部分兼容（需要配置）

| ORM | 语言 | 版本 | 状态 | 限制 | 解决方案 |
|-----|------|------|------|------|----------|
| **SQLAlchemy** | Python | 2.0+ | ⚠️ 部分兼容 | DDL 内省可能不完整 | 使用 `create_all()` 而非 `reflect()` |
| **Django ORM** | Python | 4.2+ | ⚠️ 部分兼容 | 迁移需要 `--fake` | 手动编写迁移文件 |
| **Hibernate** | Java | 6.x | ⚠️ 部分兼容 | Schema 验证可能失败 | 设置 `hibernate.hbm2ddl.auto=none` |
| **Prisma** | Node.js/TS | 5.x | ⚠️ 部分兼容 | `prisma db push` 受限 | 使用 `prisma migrate` |
| **TypeORM** | Node.js/TS | 0.3+ | ⚠️ 部分兼容 | 自动迁移受限 | 手动编写迁移 |
| **GORM** | Go | 1.25+ | ⚠️ 部分兼容 | `AutoMigrate` 受限 | 手动定义 schema |

## 已实现的系统目录表

### pg_catalog 核心表

| 表名 | 用途 | 完整度 | 说明 |
|------|------|--------|------|
| `pg_type` | 类型映射 | ✅ 完整 | 所有基础类型 + JSONB/UUID/数组 |
| `pg_class` | 表/索引列表 | ✅ 完整 | 支持 relkind 过滤 |
| `pg_attribute` | 列定义 | ✅ 完整 | 包含类型、默认值、NOT NULL |
| `pg_namespace` | Schema 列表 | ✅ 完整 | public + pg_catalog + information_schema |
| `pg_index` | 索引信息 | ✅ 完整 | 主键、唯一索引、普通索引 |
| `pg_constraint` | 约束定义 | ✅ 完整 | PK/FK/UNIQUE/CHECK |
| `pg_database` | 数据库列表 | ✅ 完整 | 当前数据库信息 |
| `pg_settings` | 配置参数 | ✅ 完整 | GUC 参数 |
| `pg_proc` | 函数列表 | ✅ 新增 | 内置函数元数据 |
| `pg_aggregate` | 聚合函数 | ✅ 新增 | COUNT/SUM/AVG/MIN/MAX |
| `pg_trigger` | 触发器 | ✅ 新增 | 空结果（触发器未实现） |
| `pg_language` | 编程语言 | ✅ 新增 | internal/c/sql/plpgsql |
| `pg_opclass` | 操作符类 | ✅ 新增 | B-tree 操作符类 |
| `pg_am` | 访问方法 | ✅ 完整 | btree/hash |
| `pg_description` | 对象注释 | ⚠️ 存根 | 返回空结果 |
| `pg_depend` | 依赖关系 | ⚠️ 存根 | 返回空结果 |
| `pg_enum` | 枚举值 | ⚠️ 存根 | 返回空结果（枚举未实现） |

### information_schema 视图

| 视图 | 用途 | 完整度 | 说明 |
|------|------|--------|------|
| `tables` | 表列表 | ✅ 完整 | table_catalog/schema/name/type |
| `columns` | 列定义 | ✅ 完整 | 包含 ordinal_position/udt_name/is_identity |
| `schemata` | Schema 列表 | ✅ 完整 | public + 系统 schema |
| `table_constraints` | 约束列表 | ✅ 完整 | PK/FK/UNIQUE/CHECK |
| `key_column_usage` | 键列映射 | ✅ 完整 | 约束 → 列映射 |
| `referential_constraints` | 外键详情 | ✅ 完整 | FK 引用关系 |
| `constraint_column_usage` | 约束列使用 | ✅ 完整 | 约束涉及的列 |
| `views` | 视图列表 | ✅ 完整 | 视图定义 |
| `sequences` | 序列列表 | ✅ 完整 | SERIAL 序列 |
| `check_constraints` | CHECK 约束 | ✅ 完整 | CHECK 表达式 |
| `triggers` | 触发器 | ✅ 新增 | 空结果（触发器未实现） |
| `routines` | 函数/过程 | ✅ 新增 | 内置函数列表 |

## ORM 特定配置

### SQLAlchemy (Python)

```python
from sqlalchemy import create_engine, MetaData

# 连接配置
engine = create_engine(
    'postgresql://falcon:password@localhost:5433/falcon',
    # 禁用 schema 反射，使用显式定义
    echo=False
)

# 推荐：显式定义模型而非反射
from sqlalchemy.orm import declarative_base
Base = declarative_base()

class User(Base):
    __tablename__ = 'users'
    id = Column(Integer, primary_key=True)
    username = Column(String(50), nullable=False)
    email = Column(String(100))
    created_at = Column(DateTime, server_default=func.now())

# 创建表
Base.metadata.create_all(engine)

# ⚠️ 避免使用 automap 反射（可能不完整）
# from sqlalchemy.ext.automap import automap_base
# Base = automap_base()
# Base.prepare(engine, reflect=True)  # 不推荐
```

### Django ORM (Python)

```python
# settings.py
DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.postgresql',
        'NAME': 'falcon',
        'USER': 'falcon',
        'PASSWORD': 'password',
        'HOST': 'localhost',
        'PORT': '5433',
    }
}

# models.py - 显式定义模型
from django.db import models

class User(models.Model):
    username = models.CharField(max_length=50)
    email = models.EmailField(max_length=100, null=True)
    created_at = models.DateTimeField(auto_now_add=True)
    
    class Meta:
        db_table = 'users'

# 迁移策略
# 1. 生成迁移文件
python manage.py makemigrations

# 2. 手动检查迁移文件，移除不支持的操作（如触发器）

# 3. 应用迁移（可能需要 --fake 跳过某些操作）
python manage.py migrate
```

### Hibernate (Java)

```xml
<!-- hibernate.cfg.xml -->
<hibernate-configuration>
    <session-factory>
        <property name="hibernate.connection.driver_class">org.postgresql.Driver</property>
        <property name="hibernate.connection.url">jdbc:postgresql://localhost:5433/falcon</property>
        <property name="hibernate.connection.username">falcon</property>
        <property name="hibernate.connection.password">password</property>
        
        <!-- 关键：禁用自动 schema 验证和更新 -->
        <property name="hibernate.hbm2ddl.auto">none</property>
        
        <!-- 使用 PostgreSQL 方言 -->
        <property name="hibernate.dialect">org.hibernate.dialect.PostgreSQLDialect</property>
    </session-factory>
</hibernate-configuration>
```

```java
// 实体定义
@Entity
@Table(name = "users")
public class User {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    
    @Column(nullable = false, length = 50)
    private String username;
    
    @Column(length = 100)
    private String email;
    
    @Column(name = "created_at", insertable = false, updatable = false)
    private Timestamp createdAt;
}
```

### Prisma (Node.js/TypeScript)

```prisma
// schema.prisma
datasource db {
  provider = "postgresql"
  url      = "postgresql://falcon:password@localhost:5433/falcon"
}

generator client {
  provider = "prisma-client-js"
}

model User {
  id        Int      @id @default(autoincrement())
  username  String   @db.VarChar(50)
  email     String?  @db.VarChar(100)
  createdAt DateTime @default(now()) @map("created_at")

  @@map("users")
}
```

```bash
# 推荐：使用 migrate 而非 db push
npx prisma migrate dev --name init

# ⚠️ 避免使用 db push（可能失败）
# npx prisma db push
```

### TypeORM (Node.js/TypeScript)

```typescript
// ormconfig.json
{
  "type": "postgres",
  "host": "localhost",
  "port": 5433,
  "username": "falcon",
  "password": "password",
  "database": "falcon",
  "synchronize": false,  // 关键：禁用自动同步
  "logging": false,
  "entities": ["src/entity/**/*.ts"],
  "migrations": ["src/migration/**/*.ts"]
}

// entity/User.ts
import { Entity, PrimaryGeneratedColumn, Column, CreateDateColumn } from "typeorm";

@Entity("users")
export class User {
    @PrimaryGeneratedColumn()
    id: number;

    @Column({ type: "varchar", length: 50 })
    username: string;

    @Column({ type: "varchar", length: 100, nullable: true })
    email: string;

    @CreateDateColumn({ name: "created_at" })
    createdAt: Date;
}
```

### GORM (Go)

```go
import (
    "gorm.io/driver/postgres"
    "gorm.io/gorm"
)

// 连接配置
dsn := "host=localhost user=falcon password=password dbname=falcon port=5433 sslmode=disable"
db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})

// 模型定义
type User struct {
    ID        uint      `gorm:"primaryKey"`
    Username  string    `gorm:"type:varchar(50);not null"`
    Email     string    `gorm:"type:varchar(100)"`
    CreatedAt time.Time `gorm:"autoCreateTime"`
}

// 手动创建表（不使用 AutoMigrate）
db.Exec(`
    CREATE TABLE IF NOT EXISTS users (
        id SERIAL PRIMARY KEY,
        username VARCHAR(50) NOT NULL,
        email VARCHAR(100),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
`)

// ⚠️ AutoMigrate 可能不完整
// db.AutoMigrate(&User{})  // 不推荐
```

## 最佳实践

### 1. Schema 管理策略

**推荐方式**：手动 SQL DDL
```sql
-- 使用标准 SQL DDL 创建 schema
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    username VARCHAR(50) NOT NULL,
    email VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_users_email ON users(email);
```

**避免**：依赖 ORM 自动迁移
```python
# ❌ 不推荐
Base.metadata.create_all(engine)  # SQLAlchemy
python manage.py migrate          # Django（可能需要手动调整）
db.AutoMigrate(&User{})           # GORM
```

### 2. 类型映射

| PostgreSQL 类型 | FalconDB 支持 | ORM 映射 |
|----------------|--------------|----------|
| `SERIAL` | ✅ | `@GeneratedValue` / `autoincrement()` |
| `VARCHAR(n)` | ✅ | `String(n)` / `varchar(n)` |
| `TEXT` | ✅ | `Text` / `text` |
| `INTEGER` | ✅ | `Integer` / `int` |
| `BIGINT` | ✅ | `BigInteger` / `bigint` |
| `BOOLEAN` | ✅ | `Boolean` / `boolean` |
| `TIMESTAMP` | ✅ | `DateTime` / `timestamp` |
| `JSONB` | ✅ | `JSONB` / `jsonb` |
| `UUID` | ✅ | `UUID` / `uuid` |
| `ARRAY` | ✅ | `ARRAY` / `array` |
| `ENUM` | ❌ | 使用 `VARCHAR` + CHECK 约束 |

### 3. 约束支持

| 约束类型 | FalconDB 支持 | ORM 使用 |
|---------|--------------|----------|
| `PRIMARY KEY` | ✅ | `@PrimaryKey` / `primary_key=True` |
| `UNIQUE` | ✅ | `@Unique` / `unique=True` |
| `NOT NULL` | ✅ | `nullable=False` |
| `CHECK` | ✅ | 手动 SQL |
| `FOREIGN KEY` | ✅ | `@ForeignKey` / `ForeignKey()` |
| `DEFAULT` | ✅ | `server_default` / `@Default` |

### 4. 索引创建

```sql
-- 推荐：手动创建索引
CREATE INDEX idx_users_username ON users(username);
CREATE UNIQUE INDEX idx_users_email ON users(email) WHERE email IS NOT NULL;

-- 复合索引
CREATE INDEX idx_users_name_email ON users(username, email);
```

### 5. 查询优化

```python
# SQLAlchemy - 使用 Core 而非 ORM 进行复杂查询
from sqlalchemy import select, text

# ✅ 推荐：使用 Core
stmt = select(User).where(User.username == 'alice')
result = session.execute(stmt).scalars().all()

# ✅ 或直接 SQL
result = session.execute(text("SELECT * FROM users WHERE username = :name"), {"name": "alice"})
```

## 故障排查

### 问题 1：ORM 启动时报错 "relation does not exist"

**原因**：ORM 尝试查询不存在的系统表

**解决**：
1. 检查 FalconDB 版本（需要 1.2.0+）
2. 查看具体查询的表名
3. 如果是 `pg_proc`/`pg_trigger` 等新增表，升级 FalconDB

### 问题 2：Schema 反射不完整

**原因**：部分 `pg_catalog` 表返回空结果

**解决**：
```python
# 不使用反射
# metadata.reflect(bind=engine)  # ❌

# 显式定义所有表
Base = declarative_base()
# ... 定义所有模型
Base.metadata.create_all(engine)  # ✅
```

### 问题 3：迁移失败

**原因**：ORM 生成的 DDL 包含不支持的特性（如触发器、物化视图）

**解决**：
1. 手动检查迁移文件
2. 移除不支持的操作
3. 使用 `--fake` 标记已应用

```bash
# Django
python manage.py migrate --fake app_name migration_name

# Prisma
npx prisma migrate resolve --applied migration_name
```

## 测试覆盖

FalconDB 包含完整的 ORM 兼容性测试套件：

```bash
# 运行 ORM 兼容性测试
cargo test --package falcon_protocol_pg orm_compat

# 运行特定 ORM 测试
cargo test --package falcon_protocol_pg test_sqlalchemy
cargo test --package falcon_protocol_pg test_django
cargo test --package falcon_protocol_pg test_hibernate
```

## 未来改进

计划在后续版本中增强的功能：

- [ ] `pg_depend` 完整实现（支持 pg_dump）
- [ ] `pg_description` 支持对象注释
- [ ] `ENUM` 类型支持
- [ ] 触发器支持
- [ ] 物化视图支持
- [ ] 完整的 `pg_stat_*` 统计视图

## 反馈

如果你在使用特定 ORM 时遇到兼容性问题，请提交 issue 并包含：

1. ORM 名称和版本
2. 失败的查询 SQL
3. 错误信息
4. 最小可复现示例

---

**文档版本**: 1.0  
**最后更新**: 2026-03-13  
**适用 FalconDB 版本**: 1.2.0+
