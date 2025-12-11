/*
 * 文件名: MySQLConnectionPool.cs
 * 作者: zengxin
 * 创建日期: 2025-12-10
 */

using System;
using System.Collections.Concurrent;
using System.Data;
using System.Threading;
using MySql.Data.MySqlClient;
using System.Security.Cryptography;

namespace ET
{
    [EnableClass]
    /// <summary>
    /// 支持可配置重连的MySQL连接池
    /// </summary>
    public class MySQLConnectionPool: IDisposable
    {
        private readonly string _connectionString;
        private readonly ConcurrentBag<MySqlConnection> _connections;
        private readonly SemaphoreSlim _semaphore;
        private int _createdCount;
        private readonly int _maxSize;
        private bool _disposed;
        private int _totalReconnects;
        private readonly Scene _scene;
        private readonly MySQLComponent _config;
        private readonly RandomNumberGenerator _rng;
        
        // 统计信息
        public int ActiveConnections => _createdCount;
        public int AvailableConnections => _connections.Count;
        public int TotalReconnects => _totalReconnects;
        
        public MySQLConnectionPool(Scene scene, string connectionString, int maxSize = 100)
        {
            if (scene == null) throw new ArgumentNullException(nameof(scene));
            
            _scene = scene;
            _connectionString = connectionString;
            _maxSize = maxSize;
            _connections = new ConcurrentBag<MySqlConnection>();
            _semaphore = new SemaphoreSlim(maxSize, maxSize);
            _rng = RandomNumberGenerator.Create();
            
            // 获取配置组件
            _config = scene.GetComponent<MySQLComponent>();
            if (_config == null)
            {
                Log.Error("MySQLComponent not found!");
                throw new Exception("MySQLComponent not found!");
            }
            
            Log.Info($"MySQL连接池初始化: 最大连接={maxSize}, 重试次数={_config.MaxReconnectRetries}, 最小连接={_config.MinPoolConnections}");
        }

        /// <summary>
        /// 获取连接（带可配置的重试）
        /// </summary>
        public async ETTask<MySqlConnection> GetConnectionAsync()
        {
            int retryCount = 0;
            int maxRetries = _config.MaxReconnectRetries;
            
            while (!_disposed)
            {
                await _semaphore.WaitAsync();
                
                try
                {
                    if (_connections.TryTake(out var connection))
                    {
                        // 验证连接是否可用
                        if (await IsConnectionAliveAsync(connection))
                            return connection;
                            
                        // 连接已死，创建新的
                        Interlocked.Decrement(ref _createdCount);
                        SafeClose(connection);
                        Log.Debug("从池中取出死连接，将创建新连接");
                    }
                    
                    // 创建新连接（带重试）
                    return await CreateNewConnectionWithRetryAsync();
                }
                catch (Exception ex)
                {
                    Log.Warning($"获取连接失败 (尝试 {retryCount + 1}/{maxRetries + 1}): {ex.Message}");
                    _semaphore.Release();
                    
                    retryCount++;
                    
                    if (retryCount > maxRetries)
                    {
                        Log.Error($"获取连接达到最大重试次数 {maxRetries}，放弃重试");
                        throw new Exception($"Failed to get MySQL connection after {maxRetries} retries", ex);
                    }
                    
                    // 计算等待时间
                    int delay = CalculateReconnectDelay(retryCount);
                    Log.Debug($"等待 {delay}ms 后重试获取连接...");
                    await WaitAsync(delay);
                }
            }
            
            throw new ObjectDisposedException("MySQLConnectionPool");
        }

        /// <summary>
        /// 释放连接回池
        /// </summary>
        public void ReleaseConnection(MySqlConnection connection)
        {
            if (connection == null || _disposed)
                return;
                
            try
            {
                if (connection.State == ConnectionState.Open)
                {
                    _connections.Add(connection);
                }
                else
                {
                    Interlocked.Decrement(ref _createdCount);
                    SafeClose(connection);
                    Log.Debug("释放时发现死连接，已关闭");
                }
            }
            catch (Exception ex)
            {
                Log.Error($"释放连接异常: {ex.Message}");
                Interlocked.Decrement(ref _createdCount);
                SafeClose(connection);
            }
            finally
            {
                _semaphore.Release();
            }
        }

        /// <summary>
        /// 创建新连接（带重试机制）
        /// </summary>
        private async ETTask<MySqlConnection> CreateNewConnectionWithRetryAsync()
        {
            int retryCount = 0;
            int maxRetries = _config.MaxReconnectRetries;
            
            while (!_disposed)
            {
                try
                {
                    _config.TotalReconnectAttempts++;
                    
                    var connection = new MySqlConnection(_connectionString);
                    await connection.OpenAsync();
                    
                    // 设置连接参数
                    await SetConnectionParamsAsync(connection);
                    
                    Interlocked.Increment(ref _createdCount);
                    _config.SuccessfulReconnects++;
                    
                    Log.Debug($"创建MySQL连接成功 (尝试 {retryCount + 1})，当前总数: {_createdCount}");
                    
                    return connection;
                }
                catch (Exception ex)
                {
                    retryCount++;
                    
                    if (retryCount > maxRetries)
                    {
                        _config.FailedReconnects++;
                        Log.Error($"创建连接达到最大重试次数 {maxRetries}");
                        throw new Exception($"Failed to create MySQL connection after {maxRetries} retries", ex);
                    }
                    
                    // 计算等待时间
                    int delay = CalculateReconnectDelay(retryCount);
                    Log.Warning($"创建MySQL连接失败 (尝试 {retryCount}/{maxRetries})，等待 {delay}ms 后重试: {ex.Message}");
                    
                    await WaitAsync(delay);
                }
            }
            
            throw new ObjectDisposedException("MySQLConnectionPool");
        }

        /// <summary>
        /// 计算重连延迟（指数退避算法）
        /// </summary>
        private int CalculateReconnectDelay(int retryCount)
        {
            if (!_config.ExponentialBackoff)
                return _config.BaseReconnectDelay;
            
            // 指数退避公式: delay = baseDelay * (2^(retryCount-1))
            int delay = _config.BaseReconnectDelay * (int)Math.Pow(2, retryCount - 1);
            
            // 添加随机抖动
            if (_config.Jitter)
            {
                byte[] randomBytes = new byte[4];
                _rng.GetBytes(randomBytes);
                int randomValue = Math.Abs(BitConverter.ToInt32(randomBytes, 0)) % 100;
                float jitterFactor = 0.8f + (randomValue / 100f * 0.4f); // 0.8-1.2的随机因子
                delay = (int)(delay * jitterFactor);
            }
            
            // 限制最大延迟
            return Math.Min(delay, _config.MaxReconnectDelay);
        }

        /// <summary>
        /// 检查连接是否存活
        /// </summary>
        private async ETTask<bool> IsConnectionAliveAsync(MySqlConnection connection)
        {
            if (connection.State != ConnectionState.Open)
                return false;
                
            try
            {
                using var cmd = new MySqlCommand("SELECT 1", connection);
                cmd.CommandTimeout = 1;
                var result = await cmd.ExecuteScalarAsync();
                return result?.ToString() == "1";
            }
            catch
            {
                return false;
            }
        }

        /// <summary>
        /// 健康检查并修复死连接
        /// </summary>
        public async ETTask HealthCheckAsync()
        {
            if (_disposed || !_config.EnableHealthCheck) return;
            
            try
            {
                var connections = _connections.ToArray();
                int deadConnections = 0;
                
                foreach (var conn in connections)
                {
                    if (!await IsConnectionAliveAsync(conn))
                    {
                        if (_connections.TryTake(out var deadConn))
                        {
                            Interlocked.Decrement(ref _createdCount);
                            SafeClose(deadConn);
                            deadConnections++;
                        }
                    }
                }
                
                if (deadConnections > 0)
                {
                    Log.Debug($"健康检查移除 {deadConnections} 个死连接");
                }
                
                // 保持最小连接数
                int targetConnections = Math.Max(_config.MinPoolConnections, _maxSize / 10);
                targetConnections = Math.Min(targetConnections, _maxSize);
                
                int attempts = 0;
                int maxAttempts = Math.Min(3, _config.HealthCheckMaxRetries);
                
                while (_createdCount < targetConnections && 
                       _connections.Count < targetConnections && 
                       attempts < maxAttempts)
                {
                    try
                    {
                        var newConn = await CreateNewConnectionWithRetryAsync();
                        ReleaseConnection(newConn);
                        Log.Debug($"健康检查添加连接，当前总数: {_createdCount}");
                        break;
                    }
                    catch (Exception ex)
                    {
                        attempts++;
                        if (attempts >= maxAttempts)
                        {
                            Log.Warning($"健康检查创建连接失败，已达到最大尝试次数 {maxAttempts}: {ex.Message}");
                            break;
                        }
                        
                        await WaitAsync(CalculateReconnectDelay(attempts));
                    }
                }
            }
            catch (Exception ex)
            {
                Log.Error($"健康检查异常: {ex}");
            }
        }

        /// <summary>
        /// 设置连接参数
        /// </summary>
        private async ETTask SetConnectionParamsAsync(MySqlConnection connection)
        {
            try
            {
                using var cmd = new MySqlCommand("SET SESSION wait_timeout=28800", connection);
                await cmd.ExecuteNonQueryAsync();
                
                using var cmd2 = new MySqlCommand("SET NAMES utf8mb4", connection);
                await cmd2.ExecuteNonQueryAsync();
                
                using var cmd3 = new MySqlCommand("SET sql_mode='STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION'", connection);
                await cmd3.ExecuteNonQueryAsync();
            }
            catch (Exception ex)
            {
                Log.Warning($"设置连接参数失败: {ex.Message}");
            }
        }

        /// <summary>
        /// 安全关闭连接
        /// </summary>
        private void SafeClose(MySqlConnection connection)
        {
            try
            {
                if (connection?.State == ConnectionState.Open)
                {
                    connection.Close();
                }
                connection?.Dispose();
            }
            catch (Exception ex)
            {
                Log.Warning($"关闭连接异常: {ex.Message}");
            }
        }

        /// <summary>
        /// 强制重连所有连接
        /// </summary>
        public async ETTask ForceReconnectAllAsync()
        {
            Log.Info("强制重连所有MySQL连接...");
            
            // 清空连接池
            while (_connections.TryTake(out var conn))
            {
                SafeClose(conn);
            }
            
            _createdCount = 0;
            _totalReconnects++;
            _config.TotalReconnectAttempts++;
            
            // 重新创建最小连接数
            int targetConnections = Math.Min(_config.MinPoolConnections, _maxSize);
            
            for (int i = 0; i < targetConnections; i++)
            {
                try
                {
                    var conn = await CreateNewConnectionWithRetryAsync();
                    ReleaseConnection(conn);
                    await WaitAsync(100); // 避免瞬间创建过多连接
                }
                catch (Exception ex)
                {
                    Log.Error($"强制重连创建连接失败: {ex.Message}");
                    break;
                }
            }
            
            Log.Info($"强制重连完成，总重连次数: {_totalReconnects}，当前连接数: {_createdCount}");
        }

        /// <summary>
        /// 使用ET的TimerComponent进行等待
        /// </summary>
        private async ETTask WaitAsync(long time)
        {
            if (_scene == null) 
                return;
            await _scene.GetComponent<TimerComponent>().WaitAsync(time);
        }

        /// <summary>
        /// 释放资源
        /// </summary>
        public void Dispose()
        {
            if (_disposed) return;
        
            _disposed = true;
            
            Log.Info("开始释放MySQL连接池...");
            
            // 释放所有连接
            int closedCount = 0;
            while (_connections.TryTake(out var connection))
            {
                SafeClose(connection);
                closedCount++;
            }
            
            _semaphore?.Dispose();
            _rng?.Dispose();
            
            Log.Info($"MySQL连接池已释放，关闭了 {closedCount} 个连接");
        }
    }
}