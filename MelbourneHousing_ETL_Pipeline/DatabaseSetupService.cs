using Dapper;
using Npgsql;

public class DatabaseSetupService
{
    private readonly string _serverConnString;
    private readonly string _targetDbConnString;

    public DatabaseSetupService(string serverConnString, string targetDbConnString)
    {
        _serverConnString = serverConnString;
        _targetDbConnString = targetDbConnString;
    }

    public void InitializeDatabase()
    {
        Console.WriteLine("Cтворення бази даних");
        using (var conn = new NpgsqlConnection(_serverConnString))
        {
            conn.Open();
            bool exists = conn
                .ExecuteScalar<bool>(@"
                    SELECT EXISTS (
                        SELECT 1 
                        FROM pg_database 
                        WHERE datname = 'melbourne_db')");
            if (!exists)
            {
                conn.Execute("CREATE DATABASE melbourne_db");
            }
        }

        Console.WriteLine("Створення таблиць");
        using (var conn = new NpgsqlConnection(_targetDbConnString))
        {
            conn.Open();
            string sql = @"
                CREATE TABLE IF NOT EXISTS ""region"" (
                   ""regionId"" SERIAL PRIMARY KEY,
                   ""regionName"" VARCHAR(100) UNIQUE
                );

                CREATE TABLE IF NOT EXISTS ""location"" (
                    ""locationId"" SERIAL PRIMARY KEY,
                    ""postCode"" VARCHAR(10),
                    ""suburbName"" VARCHAR(100),
                    ""regionId"" INTEGER REFERENCES ""region""(""regionId""),
                    ""totalPropertiesInSuburb"" INTEGER
                );

                CREATE TABLE IF NOT EXISTS ""buildingType"" (
                    ""buildingTypeId"" SERIAL PRIMARY KEY,
                    ""buildingTypeCode"" CHAR(1) UNIQUE,
                    ""buildingTypeName"" VARCHAR(50)
                );

                CREATE TABLE IF NOT EXISTS ""saleMethod"" (
                    ""saleMethodId"" SERIAL PRIMARY KEY,
                    ""saleMethodCode"" VARCHAR(2) UNIQUE,
                    ""saleMethodName"" VARCHAR(50)
                );

                CREATE TABLE IF NOT EXISTS ""building"" (
                    ""buildingId"" SERIAL PRIMARY KEY,
                    ""streetAdress"" VARCHAR(100),
                    ""totalRooms"" SMALLINT,
                    ""buildingTypeId"" INTEGER REFERENCES ""buildingType""(""buildingTypeId""),
                    ""distanceToCbd"" FLOAT,
                    ""bedroomCount"" SMALLINT,
                    ""bathroomCount"" SMALLINT,
                    ""parkingSpots"" SMALLINT,
                    ""landAreaSqm"" FLOAT,
                    ""livingAreaSqm"" FLOAT,
                    ""yearBuilt"" SMALLINT,
                    ""geoLattitude"" FLOAT,
                    ""geoLongtitude"" FLOAT,
                    ""locationId"" INTEGER REFERENCES ""location""(""locationId"")
                );

                CREATE TABLE IF NOT EXISTS ""sale"" (
                    ""saleId"" SERIAL PRIMARY KEY,
                    ""salePrice"" DECIMAL(15, 2),
                    ""saleMethodId"" INTEGER REFERENCES ""saleMethod""(""saleMethodId""),
                    ""saleDate"" DATE,
                    ""buildingId"" INTEGER REFERENCES ""building""(""buildingId"")
                );";
            conn.Execute(sql);
        }
    }
}