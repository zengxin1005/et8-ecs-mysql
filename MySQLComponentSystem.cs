/*
 * 文件名: MySQLComponentSystem.cs
 * 作者: zengxin
 * 创建日期: 2026-2-25
 */
using Dapper;
using MySqlConnector;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.Linq;

namespace ET.Server
{
    [EntitySystemOf(typeof(MySQLComponent))]
    [FriendOf(typeof(MySQLComponent))]
    public static partial class MySQLComponentSystem
    {
        #region 生命周期
        
        [EntitySystem]
        private static void Awake(this MySQLComponent self, string connectionString, string dbName)
        {
            self.ConnectionString = connectionString;
            self.DatabaseName = dbName;
            
            // 测试连接是否正常
            try
            {
                using var conn = new MySqlConnection(connectionString);
                conn.Open();
                Log.Info($"MySQLComponent 初始化成功: {dbName}, 使用内置连接池");
            }
            catch (Exception ex)
            {
                Log.Error($"MySQLComponent 初始化失败: {dbName}, {ex.Message}");
            }
        }
        
        [EntitySystem]
        private static void Destroy(this MySQLComponent self)
        {
            // 不需要手动清理，MySqlConnector 内置连接池自动管理
            Log.Info($"MySQLComponent 已销毁: {self.DatabaseName}");
        }
        
        #endregion
        
        #region 连接管理（使用 MySqlConnector 内置连接池）
        
        /// <summary>
        /// 获取连接 - 直接创建，让驱动自己处理连接池
        /// </summary>
        public static MySqlConnection RentConnection(this MySQLComponent self)
        {
            try
            {
                var conn = new MySqlConnection(self.ConnectionString);
                conn.Open();
                return conn;
            }
            catch (Exception ex)
            {
                Log.Error($"创建MySQL连接失败: {ex.Message}");
                throw;
            }
        }
        
        /// <summary>
        /// 归还连接 - 直接关闭，驱动自动归还到池中
        /// </summary>
        public static void ReturnConnection(this MySQLComponent self, MySqlConnection conn)
        {
            try
            {
                if (conn != null)
                {
                    if (conn.State != System.Data.ConnectionState.Closed)
                    {
                        conn.Close();
                    }
                    conn.Dispose();
                }
            }
            catch (Exception ex)
            {
                Log.Warning($"关闭MySQL连接时出错: {ex.Message}");
            }
        }
        
        /// <summary>
        /// 使用连接执行函数（自动归还）
        /// </summary>
        public static async ETTask<T> WithConnection<T>(this MySQLComponent self, Func<MySqlConnection, Task<T>> func)
        {
            var conn = self.RentConnection();
            try
            {
                return await func(conn);
            }
            finally
            {
                self.ReturnConnection(conn);
            }
        }
        
        /// <summary>
        /// 使用连接执行函数（无返回值）
        /// </summary>
        public static async ETTask WithConnection(this MySQLComponent self, Func<MySqlConnection, Task> func)
        {
            var conn = self.RentConnection();
            try
            {
                await func(conn);
            }
            finally
            {
                self.ReturnConnection(conn);
            }
        }
        
        #endregion
        
        #region 查询 (Query) - 使用 Dapper
        
        /// <summary>
        /// 根据ID查询单个实体
        /// </summary>
        public static async ETTask<T> Query<T>(this MySQLComponent self, long id, string tableName = null) where T : class
        {
            tableName ??= typeof(T).Name;
            string sql = $"SELECT * FROM `{tableName}` WHERE Id = @id LIMIT 1";
            
            return await self.WithConnection(async conn =>
            {
                return await conn.QueryFirstOrDefaultAsync<T>(sql, new { id });
            });
        }
        
        /// <summary>
        /// 根据条件查询列表
        /// </summary>
        public static async ETTask<List<T>> Query<T>(this MySQLComponent self, string where, object param = null, string tableName = null) where T : class
        {
            tableName ??= typeof(T).Name;
            string sql = $"SELECT * FROM `{tableName}`";
            if (!string.IsNullOrEmpty(where))
            {
                sql += $" WHERE {where}";
            }
            
            return await self.WithConnection(async conn =>
            {
                var result = await conn.QueryAsync<T>(sql, param);
                return result.AsList();
            });
        }
        
        /// <summary>
        /// 根据条件查询单个
        /// </summary>
        public static async ETTask<T> QuerySingle<T>(this MySQLComponent self, string where, object param = null, string tableName = null) where T : class
        {
            tableName ??= typeof(T).Name;
            string sql = $"SELECT * FROM `{tableName}`";
            if (!string.IsNullOrEmpty(where))
            {
                sql += $" WHERE {where}";
            }
            sql += " LIMIT 1";
            
            return await self.WithConnection(async conn =>
            {
                return await conn.QueryFirstOrDefaultAsync<T>(sql, param);
            });
        }
        
        /// <summary>
        /// 带排序和限制的查询（类似MongoDB的QueryWithSort）
        /// </summary>
        public static async ETTask<List<T>> QueryWithSort<T>(this MySQLComponent self, 
            string where, 
            string orderBy, 
            bool desc = false, 
            int limit = 0, 
            object param = null, 
            string tableName = null) where T : class
        {
            tableName ??= typeof(T).Name;
            string sql = $"SELECT * FROM `{tableName}`";
            
            if (!string.IsNullOrEmpty(where))
            {
                sql += $" WHERE {where}";
            }
            
            if (!string.IsNullOrEmpty(orderBy))
            {
                sql += $" ORDER BY {orderBy} {(desc ? "DESC" : "ASC")}";
            }
            
            if (limit > 0)
            {
                sql += $" LIMIT {limit}";
            }
            
            return await self.WithConnection(async conn =>
            {
                var result = await conn.QueryAsync<T>(sql, param);
                return result.AsList();
            });
        }
        
        /// <summary>
        /// 分页查询
        /// </summary>
        public static async ETTask<(List<T> Items, int Total)> QueryPaged<T>(this MySQLComponent self,
            string where,
            string orderBy,
            int page,
            int pageSize,
            object param = null,
            string tableName = null) where T : class
        {
            tableName ??= typeof(T).Name;
            int offset = (page - 1) * pageSize;
            
            string countSql = $"SELECT COUNT(*) FROM `{tableName}`";
            string dataSql = $"SELECT * FROM `{tableName}`";
            
            if (!string.IsNullOrEmpty(where))
            {
                countSql += $" WHERE {where}";
                dataSql += $" WHERE {where}";
            }
            
            if (!string.IsNullOrEmpty(orderBy))
            {
                dataSql += $" ORDER BY {orderBy}";
            }
            
            dataSql += $" LIMIT {offset}, {pageSize}";
            
            return await self.WithConnection(async conn =>
            {
                using var multi = await conn.QueryMultipleAsync($"{countSql}; {dataSql}", param);
                int total = await multi.ReadFirstAsync<int>();
                var items = (await multi.ReadAsync<T>()).AsList();
                return (items, total);
            });
        }
        
        /// <summary>
        /// 按ID列表批量查询
        /// </summary>
        public static async ETTask<List<T>> QueryByIds<T>(this MySQLComponent self, 
            IEnumerable<long> ids, 
            string tableName = null) where T : class
        {
            tableName ??= typeof(T).Name;
            var idList = ids.ToList();
            if (!idList.Any()) return new List<T>();
            
            string sql = $"SELECT * FROM `{tableName}` WHERE Id IN @ids";
            
            return await self.WithConnection(async conn =>
            {
                var result = await conn.QueryAsync<T>(sql, new { ids = idList });
                return result.AsList();
            });
        }
        
        /// <summary>
        /// 执行自定义SQL查询（返回强类型）
        /// </summary>
        public static async ETTask<List<T>> QuerySql<T>(this MySQLComponent self, string sql, object param = null)
        {
            return await self.WithConnection(async conn =>
            {
                var result = await conn.QueryAsync<T>(sql, param);
                return result.AsList();
            });
        }
        
        /// <summary>
        /// 执行自定义SQL查询（返回dynamic）
        /// </summary>
        public static async ETTask<List<dynamic>> QueryDynamic(this MySQLComponent self, string sql, object param = null)
        {
            return await self.WithConnection(async conn =>
            {
                var result = await conn.QueryAsync(sql, param);
                return result.AsList();
            });
        }
        
        #endregion
        
        #region 写入操作 (Execute) - 使用 Dapper
        
        /// <summary>
        /// 执行SQL（Insert/Update/Delete）
        /// </summary>
        public static async ETTask<int> Execute(this MySQLComponent self, string sql, object param = null)
        {
            return await self.WithConnection(async conn =>
            {
                return await conn.ExecuteAsync(sql, param);
            });
        }
        
        /// <summary>
        /// 插入实体
        /// </summary>
        public static async ETTask<int> Insert<T>(this MySQLComponent self, T entity, string tableName = null) where T : class
        {
            tableName ??= typeof(T).Name;
            
            // 获取所有属性
            var properties = typeof(T).GetProperties()
                .Where(p => p.Name != "Id" || p.GetValue(entity)?.ToString() != "0")
                .Where(p => !p.PropertyType.IsGenericType) 
                .ToList();
            
            var columns = string.Join(", ", properties.Select(p => $"`{p.Name}`"));
            var values = string.Join(", ", properties.Select(p => $"@{p.Name}"));
            
            string sql = $"INSERT INTO `{tableName}` ({columns}) VALUES ({values})";
            
            return await self.WithConnection(async conn =>
            {
                return await conn.ExecuteAsync(sql, entity);
            });
        }
        
        /// <summary>
        /// 批量插入
        /// </summary>
        public static async ETTask<int> InsertBatch<T>(this MySQLComponent self, IEnumerable<T> entities, string tableName = null)
        {
            tableName ??= typeof(T).Name;
            var list = entities.ToList();
            if (!list.Any()) return 0;
            
            var properties = MySQLComponent.PropertiesCache.GetOrAdd(typeof(T), t =>
                t.GetProperties()
                 .Where(p => p.Name != "Id")
                 .Where(p => !p.PropertyType.IsGenericType)
                 .ToArray()
            );
            
            var columns = string.Join(", ", properties.Select(p => $"`{p.Name}`"));
            var values = string.Join(", ", properties.Select(p => $"@{p.Name}"));
            
            string sql = $"INSERT INTO `{tableName}` ({columns}) VALUES ({values})";
            
            return await self.WithConnection(async conn =>
            {
                return await conn.ExecuteAsync(sql, list);
            });
        }
        
        /// <summary>
        /// 更新实体
        /// </summary>
        public static async ETTask<int> Update<T>(this MySQLComponent self, T entity, string tableName = null) where T : class
        {
            tableName ??= typeof(T).Name;
            
            var properties = MySQLComponent.PropertiesCache.GetOrAdd(typeof(T), t =>
                t.GetProperties()
                 .Where(p => p.Name != "Id")
                 .Where(p => !p.PropertyType.IsGenericType)
                 .ToArray()
            );
            
            var setClause = string.Join(", ", properties.Select(p => $"`{p.Name}` = @{p.Name}"));
            
            string sql = $"UPDATE `{tableName}` SET {setClause} WHERE Id = @Id";
            
            return await self.WithConnection(async conn =>
            {
                return await conn.ExecuteAsync(sql, entity);
            });
        }
        
        /// <summary>
        /// 保存（Insert or Update）
        /// </summary>
        public static async ETTask<int> Save<T>(this MySQLComponent self, T entity, string tableName = null) where T : class
        {
            tableName ??= typeof(T).Name;
            
            var properties = MySQLComponent.PropertiesCache.GetOrAdd(typeof(T), t =>
                t.GetProperties()
                 .Where(p => p.CanWrite)
                 .Where(p => !p.PropertyType.IsGenericType)
                 .ToArray()
            );
            
            var columns = string.Join(", ", properties.Select(p => $"`{p.Name}`"));
            var values = string.Join(", ", properties.Select(p => $"@{p.Name}"));
            var updates = string.Join(", ", properties.Select(p => $"`{p.Name}` = @{p.Name}"));
            
            string sql = $@"
                INSERT INTO `{tableName}` ({columns}) VALUES ({values})
                ON DUPLICATE KEY UPDATE {updates}";
            
            return await self.WithConnection(async conn =>
            {
                return await conn.ExecuteAsync(sql, entity);
            });
        }
        
        /// <summary>
        /// 根据ID删除
        /// </summary>
        public static async ETTask<int> Delete<T>(this MySQLComponent self, long id, string tableName = null) where T : class
        {
            tableName ??= typeof(T).Name;
            string sql = $"DELETE FROM `{tableName}` WHERE Id = @id";
            
            return await self.WithConnection(async conn =>
            {
                return await conn.ExecuteAsync(sql, new { id });
            });
        }
        
        /// <summary>
        /// 根据条件删除
        /// </summary>
        public static async ETTask<int> Delete<T>(this MySQLComponent self, string where, object param = null, string tableName = null) where T : class
        {
            tableName ??= typeof(T).Name;
            string sql = $"DELETE FROM `{tableName}`";
            if (!string.IsNullOrEmpty(where))
            {
                sql += $" WHERE {where}";
            }
            
            return await self.WithConnection(async conn =>
            {
                return await conn.ExecuteAsync(sql, param);
            });
        }
        
        #endregion
        
        #region 事务支持
        
        /// <summary>
        /// 执行事务
        /// </summary>
        public static async ETTask<bool> ExecuteTransaction(this MySQLComponent self, Func<MySqlConnection, MySqlTransaction, Task> action)
        {
            var conn = self.RentConnection();
            var transaction = await conn.BeginTransactionAsync();
            
            try
            {
                await action(conn, transaction);
                await transaction.CommitAsync();
                return true;
            }
            catch (Exception e)
            {
                await transaction.RollbackAsync();
                Log.Error($"事务执行失败: {e}");
                return false;
            }
            finally
            {
                self.ReturnConnection(conn);
            }
        }
        
        /// <summary>
        /// 执行事务并返回结果
        /// </summary>
        public static async ETTask<T> ExecuteTransaction<T>(this MySQLComponent self, Func<MySqlConnection, MySqlTransaction, Task<T>> func)
        {
            var conn = self.RentConnection();
            var transaction = await conn.BeginTransactionAsync();
            
            try
            {
                var result = await func(conn, transaction);
                await transaction.CommitAsync();
                return result;
            }
            catch (Exception e)
            {
                await transaction.RollbackAsync();
                Log.Error($"事务执行失败: {e}");
                throw;
            }
            finally
            {
                self.ReturnConnection(conn);
            }
        }
        
        #endregion
        
        #region 标量查询
        
        /// <summary>
        /// 执行标量查询
        /// </summary>
        public static async ETTask<T> ExecuteScalar<T>(this MySQLComponent self, string sql, object param = null)
        {
            return await self.WithConnection(async conn =>
            {
                return await conn.ExecuteScalarAsync<T>(sql, param);
            });
        }
        
        /// <summary>
        /// 查询总数
        /// </summary>
        public static async ETTask<long> Count<T>(this MySQLComponent self, string where = null, object param = null, string tableName = null) where T : class
        {
            tableName ??= typeof(T).Name;
            string sql = $"SELECT COUNT(*) FROM `{tableName}`";
            if (!string.IsNullOrEmpty(where))
            {
                sql += $" WHERE {where}"; 
            }
            
            return await self.WithConnection(async conn =>
            {
                return await conn.ExecuteScalarAsync<long>(sql, param);
            });
        }
        
        /// <summary>
        /// 判断是否存在
        /// </summary>
        public static async ETTask<bool> Exists<T>(this MySQLComponent self, string where, object param = null, string tableName = null) where T : class
        {
            tableName ??= typeof(T).Name;
            string sql = $"SELECT 1 FROM `{tableName}` WHERE {where} LIMIT 1";
            
            return await self.WithConnection(async conn =>
            {
                var result = await conn.ExecuteScalarAsync<int?>(sql, param);
                return result.HasValue;
            });
        }
        
        #endregion
        
        #region 联表查询
        
        /// <summary>
        /// 两表联查
        /// </summary>
        public static async ETTask<List<TResult>> QueryMulti<T1, T2, TResult>(
            this MySQLComponent self,
            string sql,
            Func<T1, T2, TResult> map,
            string splitOn,
            object param = null)
        {
            return await self.WithConnection(async conn =>
            {
                var result = await conn.QueryAsync(sql, map, param, splitOn: splitOn);
                return result.AsList();
            });
        }

        /// <summary>
        /// 三表联查
        /// </summary>
        public static async ETTask<List<TResult>> QueryMulti<T1, T2, T3, TResult>(
            this MySQLComponent self,
            string sql,
            Func<T1, T2, T3, TResult> map,
            string splitOn,
            object param = null)
        {
            return await self.WithConnection(async conn =>
            {
                var result = await conn.QueryAsync(sql, map, param, splitOn: splitOn);
                return result.AsList();
            });
        }
        
        #endregion
    }
}