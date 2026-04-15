# et8-ecs-mysql

基于 ET8 ECS 框架的 MySQL 驱动

## 使用示例

```csharp
public static MySQLComponent GetZoneMySQL(this DBManagerComponent self, int zone)
{
    MySQLComponent mySQLComponent = self.GetChild<MySQLComponent>(zone);
    if (mySQLComponent != null)
    {
        return mySQLComponent;
    }
    StartZoneConfig startZoneConfig = StartZoneConfigCategory.Instance.Get(zone);
    if (startZoneConfig.DBConnection == "")
    {
        throw new Exception($"zone: {zone} not found mysql connect string");
    }

    mySQLComponent = self.AddChildWithId<MySQLComponent, string, string>(zone, startZoneConfig.DBConnection, startZoneConfig.DBName);
        
    return mySQLComponent;
}

public static MySQLComponent CreateZoneMySQL(this DBManagerComponent self, int zone, string connectionString, string dbName)
{
    MySQLComponent mySQLComponent = self.GetChild<MySQLComponent>(zone);
    if (mySQLComponent != null)
    {
        throw new Exception($"zone: {zone} already created mySQL");
    }
    // 连接配置示例
    // "DBConnection": "Server=localhost;Port=3306;Database=game_db;Uid=root;Pwd=123456;Pooling=true;Min Pool Size=5;Max Pool Size=100;",
    // "DBName": "game_db"
    mySQLComponent = self.AddChildWithId<MySQLComponent, string, string>(zone, connectionString, dbName);
    return mySQLComponent;
}