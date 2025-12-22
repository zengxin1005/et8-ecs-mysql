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
using System.Linq;
using System.Threading.Tasks;
using System.Diagnostics;

namespace ET
{
    [EnableClass]
    /// <summary>
    /// MySQL连接池（极简高效版）
    /// 获取时只检查State，失败时才彻底检查，不可用立即丢弃
    /// </summary>
    public class MySQLConnectionPool: IDisposable
    {
        private readonly string _connectionString;
        private readonly ConcurrentQueue<MySqlConnection> _connections;
        private readonly SemaphoreSlim _semaphore;
        private int _createdCount;
        private readonly int _maxSize;
        private bool _disposed;
        private int _totalReconnects;
        private readonly Scene _scene;
        private readonly MySQLComponent _config;
        private readonly RandomNumberGenerator _rng;
        
        // 性能统计
        private long _totalGetConnectionTime;
        private int _totalGetConnectionCount;
        private long _totalHealthCheckTime;
        private int _totalHealthCheckCount;
        private int _deadConnectionsRemoved;
        private int _connectionsRecovered;
        private int _stateCheckPassed;
        private int _stateCheckFailed;
        private int _fullValidationCount;
        private int _fullValidationPassed;
        private int _fullValidationFailed;
        
        // 统计信息
        public int ActiveConnections => _createdCount;
        public int AvailableConnections => _connections.Count;
        public int TotalReconnects => _totalReconnects;
        public int DeadConnectionsRemoved => _deadConnectionsRemoved;
        public int ConnectionsRecovered => _connectionsRecovered;
        
        public MySQLConnectionPool(Scene scene, string connectionString, int maxSize = 100)
        {
            if (scene == null) throw new ArgumentNullException(nameof(scene));
            
            _scene = scene;
            _connectionString = connectionString;
            _maxSize = maxSize;
            _connections = new ConcurrentQueue<MySqlConnection>();
            _semaphore = new SemaphoreSlim(maxSize, maxSize);
            _rng = RandomNumberGenerator.Create();
            
            _config = scene.GetComponent<MySQLComponent>();
            if (_config == null)
                throw new Exception("MySQLComponent not found!");
            
            Log.Info($"MySQL连接池初始化: 最大连接={maxSize}, 重试次数={_config.MaxReconnectRetries}");
        }

        /// <summary>
        /// 获取连接（只检查State，不执行SQL验证）
        /// </summary>
        public async ETTask<MySqlConnection> GetConnectionAsync()
        {
            var stopwatch = Stopwatch.StartNew();
            
            await _semaphore.WaitAsync();
            
            try
            {
                int maxRetries = 3;
                int retryCount = 0;
                
                // 尝试从池中获取可用连接
                while (retryCount < maxRetries && !_connections.IsEmpty)
                {
                    if (_connections.TryDequeue(out var connection))
                    {
                        // 只检查State属性（内存操作，性能开销极小）
                        if (connection.State == ConnectionState.Open)
                        {
                            Interlocked.Increment(ref _stateCheckPassed);
                            
                            stopwatch.Stop();
                            Interlocked.Add(ref _totalGetConnectionTime, stopwatch.ElapsedMilliseconds);
                            Interlocked.Increment(ref _totalGetConnectionCount);
                            
                            Log.Debug($"获取连接成功（状态检查通过），当前可用连接: {_connections.Count}");
                            return connection;
                        }
                        else
                        {
                            // State检查失败，连接已死
                            Interlocked.Increment(ref _stateCheckFailed);
                            Interlocked.Decrement(ref _createdCount);
                            Interlocked.Increment(ref _deadConnectionsRemoved);
                            SafeClose(connection);
                            
                            retryCount++;
                            Log.Debug($"连接状态检查失败，尝试下一个 ({retryCount}/{maxRetries})");
                        }
                    }
                    else
                    {
                        break;
                    }
                }
                
                // 没有可用连接或都失败了，创建新连接
                var newConn = await CreateNewConnectionAsync();
                
                stopwatch.Stop();
                Interlocked.Add(ref _totalGetConnectionTime, stopwatch.ElapsedMilliseconds);
                Interlocked.Increment(ref _totalGetConnectionCount);
                
                return newConn;
            }
            catch (Exception ex)
            {
                _semaphore.Release();
                Log.Error($"获取连接异常: {ex.Message}");
                throw;
            }
        }

        /// <summary>
        /// 释放连接（失败时才彻底检查）
        /// </summary>
        public void ReleaseConnection(MySqlConnection connection, bool executionSuccess = true)
        {
            if (connection == null || _disposed)
            {
                _semaphore.Release();
                return;
            }
            
            try
            {
                if (executionSuccess)
                {
                    // 执行成功：直接放回池中
                    _connections.Enqueue(connection);
                    Log.Debug($"连接执行成功，放回池中，当前可用连接: {_connections.Count}");
                }
                else
                {
                    // 执行失败：彻底检查连接健康状况
                    Log.Debug($"连接执行失败，进行彻底检查...");
                    bool isAlive = FullValidateConnectionAsync(connection).GetAwaiter().GetResult();
                    Interlocked.Increment(ref _fullValidationCount);
                    
                    if (isAlive)
                    {
                        // 彻底检查通过：可能是临时错误，放回池中
                        Interlocked.Increment(ref _fullValidationPassed);
                        Interlocked.Increment(ref _connectionsRecovered);
                        _connections.Enqueue(connection);
                        Log.Debug($"连接彻底检查通过，放回池中（临时错误）");
                    }
                    else
                    {
                        // 彻底检查失败：连接已死，立即丢弃
                        Interlocked.Increment(ref _fullValidationFailed);
                        Interlocked.Decrement(ref _createdCount);
                        Interlocked.Increment(ref _deadConnectionsRemoved);
                        SafeClose(connection);
                        Log.Debug($"连接彻底检查失败，立即丢弃");
                    }
                }
            }
            catch (Exception ex)
            {
                Log.Error($"释放连接异常: {ex.Message}");
                SafeClose(connection);
            }
            finally
            {
                _semaphore.Release();
            }
        }

        /// <summary>
        /// 彻底检查连接（Ping/SELECT 1）
        /// </summary>
        private async Task<bool> FullValidateConnectionAsync(MySqlConnection connection)
        {
            if (connection.State != ConnectionState.Open)
                return false;
                
            try
            {
                // 优先使用PingAsync（性能更好）
                #if NETSTANDARD2_1_OR_GREATER || NETCOREAPP3_1_OR_GREATER
                var pingMethod = connection.GetType().GetMethod("PingAsync", Type.EmptyTypes);
                if (pingMethod != null)
                {
                    var task = (Task<bool>)pingMethod.Invoke(connection, null);
                    return await task;
                }
                #endif
                
                // 兼容方案：使用SELECT 1
                using var cmd = new MySqlCommand("SELECT 1", connection);
                cmd.CommandTimeout = 2;
                var result = await cmd.ExecuteScalarAsync();
                return result?.ToString() == "1";
            }
            catch
            {
                return false;
            }
        }

        /// <summary>
        /// 创建新连接（带重试机制）
        /// </summary>
        private async ETTask<MySqlConnection> CreateNewConnectionAsync()
        {
            int retryCount = 0;
            int maxRetries = _config.MaxReconnectRetries;
            
            while (!_disposed && retryCount <= maxRetries)
            {
                try
                {
                    _config.TotalReconnectAttempts++;
                    
                    var connection = new MySqlConnection(_connectionString);
                    await connection.OpenAsync();
                    
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
            
            int delay = _config.BaseReconnectDelay * (int)Math.Pow(2, retryCount - 1);
            
            if (_config.Jitter)
            {
                byte[] randomBytes = new byte[4];
                _rng.GetBytes(randomBytes);
                int randomValue = Math.Abs(BitConverter.ToInt32(randomBytes, 0)) % 100;
                float jitterFactor = 0.8f + (randomValue / 100f * 0.4f);
                delay = (int)(delay * jitterFactor);
            }
            
            return Math.Min(delay, _config.MaxReconnectDelay);
        }

        /// <summary>
        /// 健康检查（定时执行，修复可能的问题）
        /// </summary>
        public async ETTask HealthCheckAsync()
        {
            var stopwatch = Stopwatch.StartNew();
            
            if (_disposed || !_config.EnableHealthCheck)
            {
                stopwatch.Stop();
                return;
            }
            
            try
            {
                var connections = _connections.ToArray();
                int totalConnections = connections.Length;
                
                if (totalConnections == 0)
                {
                    Log.Debug("健康检查: 连接池为空");
                    return;
                }
                
                int checkedCount = 0;
                int removedCount = 0;
                
                // 并行检查所有连接
                var checkTasks = connections.Select(FullValidateConnectionAsync).ToArray();
                var results = await Task.WhenAll(checkTasks);
                
                // 重建连接队列
                var newQueue = new ConcurrentQueue<MySqlConnection>();
                
                for (int i = 0; i < results.Length; i++)
                {
                    checkedCount++;
                    
                    if (results[i])
                    {
                        // 连接健康，加入新队列
                        newQueue.Enqueue(connections[i]);
                    }
                    else
                    {
                        // 连接不健康，移除
                        Interlocked.Decrement(ref _createdCount);
                        Interlocked.Increment(ref _deadConnectionsRemoved);
                        SafeClose(connections[i]);
                        removedCount++;
                    }
                }
                
                // 替换队列
                _connections.Clear();
                foreach (var conn in newQueue)
                {
                    _connections.Enqueue(conn);
                }
                
                if (checkedCount > 0)
                {
                    Log.Debug($"健康检查: 检查{checkedCount}个连接，移除{removedCount}个死连接，剩余{_connections.Count}个健康连接");
                }
                
                // 维护最小连接数
                await MaintainMinConnectionsAsync();
            }
            catch (Exception ex)
            {
                Log.Error($"健康检查异常: {ex}");
            }
            finally
            {
                stopwatch.Stop();
                Interlocked.Add(ref _totalHealthCheckTime, stopwatch.ElapsedMilliseconds);
                Interlocked.Increment(ref _totalHealthCheckCount);
            }
        }

        /// <summary>
        /// 维护最小连接数
        /// </summary>
        private async ETTask MaintainMinConnectionsAsync()
        {
            int targetConnections = Math.Max(_config.MinPoolConnections, _maxSize / 10);
            targetConnections = Math.Min(targetConnections, _maxSize);
            
            int attempts = 0;
            int maxAttempts = Math.Min(3, _config.HealthCheckMaxRetries);
            int created = 0;
            
            while (_createdCount < targetConnections && 
                   _connections.Count < targetConnections && 
                   attempts < maxAttempts &&
                   created < 2)
            {
                try
                {
                    var newConn = await CreateNewConnectionAsync();
                    _connections.Enqueue(newConn);
                    created++;
                    
                    Log.Debug($"健康检查添加连接，当前总数: {_createdCount}，可用连接: {_connections.Count}");
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
            while (_connections.TryDequeue(out var conn))
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
                    var conn = await CreateNewConnectionAsync();
                    _connections.Enqueue(conn);
                    await WaitAsync(100);
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
        /// 获取性能统计
        /// </summary>
        public string GetPerformanceStats()
        {
            float avgGetTime = _totalGetConnectionCount > 0 ? 
                _totalGetConnectionTime * 1.0f / _totalGetConnectionCount : 0;
            float avgHealthCheckTime = _totalHealthCheckCount > 0 ? 
                _totalHealthCheckTime * 1.0f / _totalHealthCheckCount : 0;
            
            float stateCheckPassRate = (_stateCheckPassed + _stateCheckFailed) > 0 ?
                _stateCheckPassed * 100f / (_stateCheckPassed + _stateCheckFailed) : 100f;
            
            float fullValidationPassRate = _fullValidationCount > 0 ?
                _fullValidationPassed * 100f / _fullValidationCount : 0;
                
            return $"获取连接: {avgGetTime:F2}ms/次({_totalGetConnectionCount}次), " +
                   $"健康检查: {avgHealthCheckTime:F2}ms/次({_totalHealthCheckCount}次), " +
                   $"状态检查: 通过率={stateCheckPassRate:F1}%({_stateCheckPassed}/{_stateCheckPassed+_stateCheckFailed}), " +
                   $"彻底验证: 通过率={fullValidationPassRate:F1}%({_fullValidationPassed}/{_fullValidationCount}), " +
                   $"连接恢复: {_connectionsRecovered}次, " +
                   $"死连接移除: {_deadConnectionsRemoved}次";
        }

        /// <summary>
        /// 重置性能统计
        /// </summary>
        public void ResetPerformanceStats()
        {
            _totalGetConnectionTime = 0;
            _totalGetConnectionCount = 0;
            _totalHealthCheckTime = 0;
            _totalHealthCheckCount = 0;
            _deadConnectionsRemoved = 0;
            _connectionsRecovered = 0;
            _stateCheckPassed = 0;
            _stateCheckFailed = 0;
            _fullValidationCount = 0;
            _fullValidationPassed = 0;
            _fullValidationFailed = 0;
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
            
            int closedCount = 0;
            while (_connections.TryDequeue(out var connection))
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