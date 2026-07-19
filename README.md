# 黑马点评（hm-dianping）

## 项目简介

黑马点评是一个基于 Spring Boot 的本地生活服务平台，涵盖商铺查询、优惠券秒杀、用户点赞排行等核心业务场景。项目重点实践了 Redis 在高并发场景下的多种应用，包括缓存策略设计、分布式锁、GEO 地理位置搜索、HyperLogLog UV 统计以及全局唯一 ID 生成等。

## 技术栈

| 技术 | 版本 | 说明 |
|------|------|------|
| Spring Boot | 2.3.12.RELEASE | 基础框架 |
| Spring Data Redis | 2.6.2 | Redis 操作封装 |
| Lettuce | 6.1.6.RELEASE | Redis 客户端 |
| Redisson | 3.13.6 | 分布式锁 |
| MyBatis-Plus | 3.4.3 | ORM 框架 |
| MySQL | 5.x（Connector 5.1.47） | 关系型数据库 |
| Hutool | 5.7.17 | Java 工具库 |
| Lombok | - | 简化实体类代码 |
| AspectJ | - | AOP 切面支持 |
| Commons-Pool2 | - | 连接池 |
| Nginx | - | 反向代理 / 负载均衡 |

## 功能模块

- **商铺查询**：支持按类型分页查询，集成 Redis GEO 实现按距离排序的附近商铺搜索
- **缓存策略**：
  - 缓存穿透解决方案（空值缓存 / 布隆过滤器）
  - 缓存击穿解决方案（互斥锁 / 逻辑过期）
- **分布式锁**：基于 Redis SETNX 及 Redisson 实现，解决并发安全问题
- **秒杀 / 优惠券**：一人一单校验、库存扣减的并发安全处理
- **点赞排行**：基于 Redis Sorted Set 实现点赞排行榜
- **UV 统计**：基于 Redis HyperLogLog 实现用户访问量统计
- **全局 ID 生成器**：基于 Redis 自增实现分布式全局唯一 ID（RedisIdWorker）
- **用户登录**：基于 Redis 实现分布式 Session，Token 认证

## 环境要求

| 环境 | 版本要求 |
|------|----------|
| JDK | 1.8+ |
| Redis | 5.0+（GEO 功能使用 `GEORADIUS` 需 Redis 3.2+；若使用 `GEOSEARCH` 需 Redis 6.2+） |
| MySQL | 5.7+ |
| Maven | 3.x |
| Nginx | 可选，用于反向代理 |

> **注意**：本项目当前 GEO 查询使用 `GEORADIUS`（兼容 Redis 5.x），无需 Redis 6.2+。

## 快速开始

### 1. 导入数据库

创建 MySQL 数据库并导入 SQL 脚本：

```sql
-- 创建数据库
CREATE DATABASE heimapoint DEFAULT CHARACTER SET utf8mb4;

-- 导入数据
source src/main/resources/db/hmdp.sql;
```

### 2. 配置 application.yaml

修改 `src/main/resources/application.yaml` 中的数据库和 Redis 连接信息：

```yaml
server:
  port: 8081
spring:
  datasource:
    driver-class-name: com.mysql.jdbc.Driver
    url: jdbc:mysql://127.0.0.1:3306/heimapoint?useSSL=false&serverTimezone=UTC
    username: root
    password: 你的MySQL密码
  redis:
    host: 127.0.0.1
    port: 6379
    password: 你的Redis密码
    lettuce:
      pool:
        max-active: 10
        max-idle: 10
        min-idle: 1
        time-between-eviction-runs: 10s
```

### 3. 启动项目

```bash
# 编译打包
mvn clean package -DskipTests

# 运行
java -jar target/hm-dianping-0.0.1-SNAPSHOT.jar
```

或直接在 IDE 中运行 `HmDianPingApplication.java` 主类。

### 4. 初始化 Redis 数据

启动后运行测试类 `HmDianPingApplicationTests` 中的 `loadShopData()` 方法，将商铺 GEO 数据写入 Redis。

## 项目结构

```
src/main/java/com/hmdp/
├── config/          # 配置类（Redis、Redisson、MyBatis-Plus、拦截器等）
├── controller/      # 控制器层（REST API 接口）
├── dto/             # 数据传输对象（登录表单、结果封装等）
├── entity/          # 实体类（Shop、User、Voucher、Blog 等）
├── mapper/          # MyBatis-Plus Mapper 接口
├── service/         # 业务逻辑层（接口 + impl 实现）
├── utils/           # 工具类（RedisIdWorker、CacheClient、RedisConstants 等）
└── HmDianPingApplication.java  # 启动类

src/main/resources/
├── application.yaml # 项目配置文件
├── db/
│   └── hmdp.sql     # 数据库初始化脚本
└── mapper/          # MyBatis XML 映射文件（如有）

src/test/java/com/hmdp/
└── HmDianPingApplicationTests.java  # 测试类（数据初始化、功能验证）
```
