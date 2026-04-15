/*
 * 文件名: MySQLComponent.cs
 * 作者: zengxin
 * 创建日期: 2026-2-25
 */
using System;
using System.Reflection;
using System.Collections.Concurrent;

namespace ET.Server
{
    [ChildOf(typeof(DBManagerComponent))]
    public class MySQLComponent : Entity, IAwake<string, string>, IDestroy
    {
        public string ConnectionString { get; set; }
        public string DatabaseName { get; set; }
        
        /// <summary>
        /// 属性缓存（用于批量操作，跳过 Id）
        /// </summary>
        [StaticField]
        public static readonly ConcurrentDictionary<Type, PropertyInfo[]> PropertiesCache = new();
    }
}