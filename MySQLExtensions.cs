/*
 * 文件名: MySQLExtensions.cs
 * 作者: zengxin
 * 创建日期: 2025-12-10
 */

using System;
using System.Collections.Generic;
using MySql.Data.MySqlClient;
using System.Diagnostics;
namespace ET
{
    public static class MySQLExtensions
    {
        public static MySQLComponent MySQL(this Scene scene)
        {
            var mysql = scene.GetComponent<MySQLComponent>();
            if (mysql == null)
            {
                throw new Exception("MySQLComponent未找到，请确保已添加MySQLModule并调用InitializeAsync");
            }
            return mysql;
        }
        
        public static async ETTask<List<T>> QueryListAsync<T>(this MySQLComponent self,
            string sql, params MySqlParameter[] parameters) where T : new()
        {
            var list = new List<T>();
            
            using (var reader = await self.QueryAsync(sql, parameters))
            {
                while (await reader.ReadAsync())
                {
                    var item = new T();
                    var properties = typeof(T).GetProperties();
                    
                    foreach (var prop in properties)
                    {
                        try
                        {
                            var value = reader[prop.Name];
                            if (value != DBNull.Value)
                            {
                                prop.SetValue(item, value);
                            }
                        }
                        catch
                        {
                            // 忽略映射错误
                        }
                    }
                    
                    list.Add(item);
                }
            }
            
            return list;
        }
        
        public static async ETTask<(List<T> Data, int Total)> QueryPageAsync<T>(
            this MySQLComponent db, 
            string table, 
            string where, 
            string orderBy, 
            int page, 
            int pageSize,
            params MySqlParameter[] parameters) where T : new()
        {
            if (page < 1) page = 1;
            int offset = (page - 1) * pageSize;
            
            string dataSql = $"SELECT * FROM {table} {where} {orderBy} LIMIT {pageSize} OFFSET {offset}";
            var data = await db.QueryListAsync<T>(dataSql, parameters);
            
            string countSql = $"SELECT COUNT(*) FROM {table} {where}";
            int total = await db.QueryScalarAsync<int>(countSql, parameters);
            
            return (data, total);
        }
        
        public static async ETTask<bool> TableExistsAsync(this MySQLComponent db, string tableName)
        {
            var result = await db.QueryScalarAsync<int>(
                "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = @table",
                new MySqlParameter("@table", tableName));
            
            return result > 0;
        }
        
        public static async ETTask<bool> ExecuteTransactionAsync(this MySQLComponent self,
            Func<MySqlConnection, ETTask<bool>> transactionFunc)
        {
            var stopwatch = Stopwatch.StartNew();
            self.TotalQueries++;
            
            int retryCount = 0;
            int maxRetries = self.MaxQueryRetries;
            
            while (true)
            {
                MySqlConnection connection = null;
                MySqlTransaction transaction = null;
                bool executionSuccess = false;
                
                try
                {
                    connection = await self.ConnectionPool.GetConnectionAsync();
                    transaction = await connection.BeginTransactionAsync();
                    
                    bool success = await transactionFunc(connection);
                    
                    if (success)
                    {
                        await transaction.CommitAsync();
                        executionSuccess = true;
                    }
                    else
                    {
                        await transaction.RollbackAsync();
                        executionSuccess = false;
                    }
                    
                    self.ConnectionPool.ReleaseConnection(connection, executionSuccess);
                    
                    stopwatch.Stop();
                    self.TotalQueryTime += stopwatch.ElapsedMilliseconds;
                    return success;
                }
                catch (Exception ex) when (MySQLSystem.IsConnectionException(ex))
                {
                    self.FailedQueries++;
                    retryCount++;
                    
                    if (retryCount > maxRetries)
                    {
                        Log.Error($"事务重试达到最大次数 {maxRetries}: {ex.Message}");
                        try { transaction?.Rollback(); } catch { }
                        self.ConnectionPool.ReleaseConnection(connection, false);
                        throw;
                    }
                    
                    Log.Warning($"事务执行时连接断开，第{retryCount}次重试: {ex.Message}");
                    try { transaction?.Rollback(); } catch { }
                    self.ConnectionPool.ReleaseConnection(connection, false);
                    
                    int delay = self.QueryRetryBaseDelay * (int)Math.Pow(2, retryCount - 1);
                    await MySQLSystem.WaitAsync(self, Math.Min(delay, 1000));
                }
                catch (Exception)
                {
                    self.FailedQueries++;
                    try { transaction?.Rollback(); } catch { }
                    self.ConnectionPool.ReleaseConnection(connection, false);
                    throw;
                }
            }
        }
        
        public static async ETTask<long> InsertAsync<T>(this MySQLComponent db, 
            T entity, string tableName = null) where T : class
        {
            var type = typeof(T);
            tableName ??= type.Name;
            
            var properties = type.GetProperties();
            var fields = new List<string>();
            var values = new List<string>();
            var parameters = new List<MySqlParameter>();
            
            foreach (var prop in properties)
            {
                if (prop.Name.Equals("Id", StringComparison.OrdinalIgnoreCase))
                    continue;
                    
                var value = prop.GetValue(entity);
                if (value == null) continue;
                
                fields.Add(prop.Name);
                values.Add($"@{prop.Name}");
                parameters.Add(new MySqlParameter($"@{prop.Name}", value));
            }
            
            string sql = $"INSERT INTO {tableName} ({string.Join(",", fields)}) VALUES ({string.Join(",", values)})";
            
            await db.ExecuteAsync(sql, parameters.ToArray());
            
            return await db.QueryScalarAsync<long>("SELECT LAST_INSERT_ID()");
        }
    }
}