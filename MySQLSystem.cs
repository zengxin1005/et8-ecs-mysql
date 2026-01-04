/*
 * 文件名: MySQLSystem.cs
 * 作者: zengxin
 * 创建日期: 2025-12-10
 */

using System;
using System.Data;
using System.Diagnostics;
using MySql.Data.MySqlClient;

namespace ET
{
    [EntitySystemOf(typeof(MySQLComponent))]
    [FriendOf(typeof(MySQLComponent))]
    public static partial class MySQLSystem  
    {
        [Invoke(TimerInvokeType.MySQLChecker)]
        public class MySQLConnectionChecker: ATimer<MySQLComponent>
        {
            protected override void Run(MySQLComponent self)
            {
                try
                {
                    self.ConnectionPool.HealthCheckAsync().Coroutine();
                }
                catch (Exception e)
                {
                    Log.Error($"MySQL健康检查定时器错误: {self.Id}\n{e}");
                }
            }
        }

        [EntitySystem]
        private static void Awake(this MySQLComponent self)
        {
            Log.Debug("MySQLComponent Awake");
            
            if (self.Scene().GetComponent<TimerComponent>() == null)
            {
                self.Scene().AddComponent<TimerComponent>();
            }
            
            if (self.EnableHealthCheck)
            {
                self.CheckTimer = self.Scene().GetComponent<TimerComponent>()
                    .NewRepeatedTimer(self.HealthCheckInterval, TimerInvokeType.MySQLChecker, self);
                Log.Info($"MySQL健康检查已启用，间隔: {self.HealthCheckInterval}ms");
            }
        }

        [EntitySystem]
        private static void Destroy(this MySQLComponent self)
        {
            try
            {
                self.Scene().GetComponent<TimerComponent>()?.Remove(ref self.CheckTimer);
                self.ConnectionPool?.Dispose();
                self.ConnectionPool = null;
                
                Log.Info($"MySQL组件已销毁，统计: 查询成功率={self.GetQuerySuccessRate():F1}%, " +
                        $"重连成功率={self.GetReconnectSuccessRate():F1}%");
            }
            catch (Exception ex)
            {
                Log.Error($"MySQL销毁异常: {ex.Message}");
            }
        }
        
        public static async ETTask InitializeAsync(this MySQLComponent self)
        {
            try
            {
                Log.Info("开始初始化MySQL连接池...");
                
                self.ConnectionPool = new MySQLConnectionPool(
                    self.Scene(),
                    self.ConnectionString, 
                    self.MaxPoolSize);
                
                await WarmupPoolAsync(self);
                
                Log.Info($"MySQL初始化完成，配置: 最大连接={self.MaxPoolSize}, " +
                        $"重试次数={self.MaxReconnectRetries}, " +
                        $"最小连接={self.MinPoolConnections}");
            }
            catch (Exception ex)
            {
                Log.Error($"MySQL初始化失败: {ex.Message}");
                throw;
            }
        }
        
        public static async ETTask ReloadConfigAsync(this MySQLComponent self)
        {
            try
            {
                Log.Info("开始重载MySQL配置...");
                
                self.Scene().GetComponent<TimerComponent>()?.Remove(ref self.CheckTimer);
                
                var oldPool = self.ConnectionPool;
                
                self.ConnectionPool = new MySQLConnectionPool(
                    self.Scene(),
                    self.ConnectionString, 
                    self.MaxPoolSize);
                
                await WarmupPoolAsync(self);
                
                oldPool?.Dispose();
                
                if (self.EnableHealthCheck)
                {
                    self.CheckTimer = self.Scene().GetComponent<TimerComponent>()
                        .NewRepeatedTimer(self.HealthCheckInterval, TimerInvokeType.MySQLChecker, self);
                }
                
                Log.Info("MySQL配置重载完成");
            }
            catch (Exception ex)
            {
                Log.Error($"MySQL配置重载失败: {ex.Message}");
                throw;
            }
        }
        
        // ========== 查询执行方法 ==========
        
        public static async ETTask<MySqlDataReader> QueryAsync(this MySQLComponent self, 
            string sql, params MySqlParameter[] parameters)
        {
            var stopwatch = Stopwatch.StartNew();
            self.TotalQueries++;
            
            int retryCount = 0;
            int maxRetries = self.MaxQueryRetries;
            
            while (true)
            {
                MySqlConnection connection = null;

                
                try
                {
                    connection = await self.ConnectionPool.GetConnectionAsync();
                    var command = CreateCommand(self, connection, sql, parameters);
                    
                    var reader = await command.ExecuteReaderAsync(CommandBehavior.CloseConnection);
                    

                    stopwatch.Stop();
                    self.TotalQueryTime += stopwatch.ElapsedMilliseconds;
                    return reader;
                }
                catch (Exception ex) when (IsConnectionException(ex))
                {
                    self.FailedQueries++;
                    retryCount++;
                    
                    if (retryCount > maxRetries)
                    {
                        Log.Error($"查询重试达到最大次数 {maxRetries}: {ex.Message}");
                        self.ConnectionPool.ReleaseConnection(connection, false);
                        throw;
                    }
                    
                    Log.Warning($"查询时连接断开，第{retryCount}次重试: {ex.Message}");
                    self.ConnectionPool.ReleaseConnection(connection, false);
                    
                    if (IsCriticalConnectionError(ex))
                    {
                        Log.Debug("检测到关键连接错误，清理连接池");
                        ClearConnectionPool(self);
                    }
                    
                    int delay = self.QueryRetryBaseDelay * (int)Math.Pow(2, retryCount - 1);
                    await WaitAsync(self, Math.Min(delay, 1000));
                }
                catch (Exception)
                {
                    self.FailedQueries++;
                    self.ConnectionPool.ReleaseConnection(connection, false);
                    throw;
                }
            }
        }
        
        public static async ETTask<int> ExecuteAsync(this MySQLComponent self, 
            string sql, params MySqlParameter[] parameters)
        {
            var stopwatch = Stopwatch.StartNew();
            self.TotalQueries++;
            
            int retryCount = 0;
            int maxRetries = self.MaxQueryRetries;
            
            while (true)
            {
                MySqlConnection connection = null;

                
                try
                {
                    connection = await self.ConnectionPool.GetConnectionAsync();
                    var command = CreateCommand(self, connection, sql, parameters);
                    var result = await command.ExecuteNonQueryAsync();
                    

                    self.ConnectionPool.ReleaseConnection(connection, true);
                    
                    stopwatch.Stop();
                    self.TotalQueryTime += stopwatch.ElapsedMilliseconds;
                    return result;
                }
                catch (Exception ex) when (IsConnectionException(ex))
                {
                    self.FailedQueries++;
                    retryCount++;
                    
                    if (retryCount > maxRetries)
                    {
                        Log.Error($"执行重试达到最大次数 {maxRetries}: {ex.Message}");
                        self.ConnectionPool.ReleaseConnection(connection, false);
                        throw;
                    }
                    
                    Log.Warning($"执行时连接断开，第{retryCount}次重试: {ex.Message}");
                    self.ConnectionPool.ReleaseConnection(connection, false);
                    
                    if (IsCriticalConnectionError(ex))
                    {
                        Log.Debug("检测到关键连接错误，清理连接池");
                        ClearConnectionPool(self);
                    }
                    
                    int delay = self.QueryRetryBaseDelay * (int)Math.Pow(2, retryCount - 1);
                    await WaitAsync(self, Math.Min(delay, 1000));
                }
                catch (Exception)
                {
                    self.FailedQueries++;
                    self.ConnectionPool.ReleaseConnection(connection, false);
                    throw;
                }
            }
        }
        
        public static async ETTask<T> QueryScalarAsync<T>(this MySQLComponent self, 
            string sql, params MySqlParameter[] parameters)
        {
            var stopwatch = Stopwatch.StartNew();
            self.TotalQueries++;
            
            int retryCount = 0;
            int maxRetries = self.MaxQueryRetries;
            
            while (true)
            {
                MySqlConnection connection = null;

                
                try
                {
                    connection = await self.ConnectionPool.GetConnectionAsync();
                    var command = CreateCommand(self, connection, sql, parameters);
                    var result = await command.ExecuteScalarAsync();
                    
  
                    self.ConnectionPool.ReleaseConnection(connection, true);
                    
                    stopwatch.Stop();
                    self.TotalQueryTime += stopwatch.ElapsedMilliseconds;
                    
                    if (result == null || result == DBNull.Value)
                        return default;
                    
                    return (T)Convert.ChangeType(result, typeof(T));
                }
                catch (Exception ex) when (IsConnectionException(ex))
                {
                    self.FailedQueries++;
                    retryCount++;
                    
                    if (retryCount > maxRetries)
                    {
                        Log.Error($"标量查询重试达到最大次数 {maxRetries}: {ex.Message}");
                        self.ConnectionPool.ReleaseConnection(connection, false);
                        throw;
                    }
                    
                    Log.Warning($"标量查询时连接断开，第{retryCount}次重试: {ex.Message}");
                    self.ConnectionPool.ReleaseConnection(connection, false);
                    
                    if (IsCriticalConnectionError(ex))
                    {
                        Log.Debug("检测到关键连接错误，清理连接池");
                        ClearConnectionPool(self);
                    }
                    
                    int delay = self.QueryRetryBaseDelay * (int)Math.Pow(2, retryCount - 1);
                    await WaitAsync(self, Math.Min(delay, 1000));
                }
                catch (Exception ex)
                {
                    self.FailedQueries++;
                    // 连接很可能是好的（除非是TimeoutException等边界情况）
                    bool connectionIsHealthy = 
                    !(ex is TimeoutException) &&
                    connection?.State == ConnectionState.Open;
                    
                    self.ConnectionPool.ReleaseConnection(connection, connectionIsHealthy);
                    stopwatch.Stop();
                    self.TotalQueryTime += stopwatch.ElapsedMilliseconds;
                    
                    // 记录详细的错误信息
                    if (ex is MySqlException mysqlEx)
                    {
                        Log.Error($"MySQL错误 [{mysqlEx.Number}]: {mysqlEx.Message}\nSQL: {sql}");
                    }
                    
                    throw;
                }
            }
        }
        
        // ========== 辅助方法 ==========
        
        private static async ETTask WarmupPoolAsync(MySQLComponent self)
        {
            Log.Info("开始预热MySQL连接池...");
            
            int successCount = 0;
            int targetCount = Math.Min(3, self.MinPoolConnections);
            
            for (int i = 0; i < targetCount; i++)
            {
                try
                {
                    var conn = await self.ConnectionPool.GetConnectionAsync();
                    self.ConnectionPool.ReleaseConnection(conn, true);
                    successCount++;
                    Log.Debug($"预热连接 {i + 1}/{targetCount} 成功");
                }
                catch (Exception ex)
                {
                    Log.Error($"预热连接失败 {i + 1}/{targetCount}: {ex.Message}");
                    
                    if (i < targetCount - 1)
                    {
                        await WaitAsync(self, 500);
                    }
                }
            }
            
            Log.Info($"预热完成: {successCount}/{targetCount}");
        }
        
        private static MySqlCommand CreateCommand(MySQLComponent self,
            MySqlConnection connection, string sql, MySqlParameter[] parameters)
        {
            var command = new MySqlCommand(sql, connection);
            
            if (parameters != null && parameters.Length > 0)
            {
                command.Parameters.AddRange(parameters);
            }
            
            command.CommandTimeout = self.CommandTimeout;
            return command;
        }
        
        public static bool IsConnectionException(Exception ex)
        {
            if (ex is MySqlException mysqlEx)
            {
                int[] connectionErrorCodes = { 
                    2002, 2003, 2006, 2013, 2026, 2048, 2055, 
                    1042, 1043, 1044, 1045, 1046, 1047, 1048, 1049 
                };
                
                foreach (var code in connectionErrorCodes)
                {
                    if (mysqlEx.Number == code)
                        return true;
                }
            }
            
            return ex is TimeoutException || 
                   ex.Message.Contains("connection", StringComparison.OrdinalIgnoreCase) ||
                   ex.Message.Contains("timeout", StringComparison.OrdinalIgnoreCase) ||
                   ex.Message.Contains("连接", StringComparison.OrdinalIgnoreCase);
        }
        
        private static bool IsCriticalConnectionError(Exception ex)
        {
            if (ex is MySqlException mysqlEx)
            {
                int[] criticalErrorCodes = { 2006, 2013, 1040, 1042, 1043, 1045 };
                foreach (var code in criticalErrorCodes)
                {
                    if (mysqlEx.Number == code)
                        return true;
                }
            }
            return false;
        }
        
        private static void ClearConnectionPool(MySQLComponent self)
        {
            try
            {
                self.ConnectionPool.ForceReconnectAllAsync().Coroutine();
            }
            catch (Exception ex)
            {
                Log.Warning($"清理连接池失败: {ex.Message}");
            }
        }
        
        public static void SafeClose(MySqlConnection connection)
        {
            try
            {
                connection?.Close();
                connection?.Dispose();
            }
            catch
            {
                // 忽略关闭错误
            }
        }
        
        public static async ETTask WaitAsync(MySQLComponent self, long time)
        {
            await self.Scene().GetComponent<TimerComponent>().WaitAsync(time);
        }
        
        public static string GetStatus(this MySQLComponent self)
        {
            if (self.ConnectionPool == null)
                return "MySQL连接池: 未初始化";
                
            var perfStats = self.ConnectionPool.GetPerformanceStats();
                
            return $"MySQL连接池: 活跃={self.ConnectionPool.ActiveConnections}, " +
                   $"可用={self.ConnectionPool.AvailableConnections}, " +
                   $"总重连={self.ConnectionPool.TotalReconnects}, " +
                   $"查询: 总数={self.TotalQueries}, 失败={self.FailedQueries}, " +
                   $"成功率={self.GetQuerySuccessRate():F1}%, " +
                   $"平均耗时={self.GetAverageQueryTime():F1}ms, " +
                   $"重连: 尝试={self.TotalReconnectAttempts}, 成功={self.SuccessfulReconnects}, " +
                   $"成功率={self.GetReconnectSuccessRate():F1}%, " +
                   $"\n性能统计: {perfStats}";
        }
        
        public static float GetReconnectSuccessRate(this MySQLComponent self)
        {
            return self.TotalReconnectAttempts > 0 ? 
                    self.SuccessfulReconnects * 100f / self.TotalReconnectAttempts : 100f;
        }
        
        public static float GetQuerySuccessRate(this MySQLComponent self)
        {
            return self.TotalQueries > 0 ? 
                    (self.TotalQueries - self.FailedQueries) * 100f / self.TotalQueries : 100f;
        }
        
        public static float GetAverageQueryTime(this MySQLComponent self)
        {
            return self.TotalQueries > 0 ? 
                    self.TotalQueryTime * 1.0f / self.TotalQueries : 0f;
        }
        
        public static void ResetStatistics(this MySQLComponent self)
        {
            self.TotalQueries = 0;
            self.FailedQueries = 0;
            self.TotalQueryTime = 0;
            self.TotalReconnectAttempts = 0;
            self.FailedReconnects = 0;
            self.SuccessfulReconnects = 0;
            self.ConnectionPool?.ResetPerformanceStats();
            Log.Info("MySQL统计信息已重置");
        }
    }
}