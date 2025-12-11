/*
 * 文件名: MySQLComponent.cs
 * 作者: zengxin
 * 创建日期: 2025-12-10
 */

namespace ET
{
    /// <summary>
    /// MySQL组件 - 配置和状态管理
    /// </summary>
    [ComponentOf(typeof(Scene))]
    public class MySQLComponent : Entity, IAwake, IDestroy
    {
        // 连接池
        public MySQLConnectionPool ConnectionPool { get; set; }
        
        // ========== 连接配置 ==========
        public string ConnectionString { get; set; } = 
            "Server=106.54.102.157;Port=3306;Database=ja_game;" +
            "Uid=root;Pwd=zenggege;Charset=utf8mb4;" +
            "Allow User Variables=True;Pooling=false;" +
            "Default Command Timeout=30;Connection Timeout=10;";
        
        public int MaxPoolSize { get; set; } = 100;
        public int CommandTimeout { get; set; } = 30;
        
        // ========== 重连配置 ==========
        public int MaxReconnectRetries { get; set; } = 3;          // 最大重试次数
        public int BaseReconnectDelay { get; set; } = 100;         // 基础重连延迟（毫秒）
        public int MaxReconnectDelay { get; set; } = 10000;        // 最大重连延迟（毫秒）
        public bool ExponentialBackoff { get; set; } = true;       // 是否使用指数退避
        public bool Jitter { get; set; } = true;                   // 是否添加随机抖动
        
        // ========== 查询重试配置 ==========
        public int MaxQueryRetries { get; set; } = 2;              // 查询最大重试次数
        public int QueryRetryBaseDelay { get; set; } = 50;         // 查询重试基础延迟
        
        // ========== 统计信息 ==========
        public int TotalQueries { get; set; }
        public int FailedQueries { get; set; }
        public long TotalQueryTime { get; set; }
        public int TotalReconnectAttempts { get; set; }            // 总重连尝试次数
        public int FailedReconnects { get; set; }                  // 失败的重连次数
        public int SuccessfulReconnects { get; set; }              // 成功的重连次数
        
        // ========== 健康检查配置 ==========
        public bool EnableHealthCheck { get; set; } = true;        // 启用健康检查
        public long HealthCheckInterval { get; set; } = 30000;     // 健康检查间隔(毫秒)
        public int MinPoolConnections { get; set; } = 3;           // 最小连接数
        public int HealthCheckMaxRetries { get; set; } = 2;        // 健康检查最大重试次数

        public long CheckTimer;
        

    }
}