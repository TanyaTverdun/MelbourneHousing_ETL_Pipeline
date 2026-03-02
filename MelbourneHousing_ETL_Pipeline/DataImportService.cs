using System.Globalization;
using CsvHelper;
using Dapper;
using Npgsql;

public class DataImportService
{
    private readonly string _targetDbConnString;

    public DataImportService(string targetDbConnString)
    {
        _targetDbConnString = targetDbConnString;
    }

    public void ImportFromCsv(string csvPath)
    {
        Console.WriteLine("Завантаження даних з CSV");
        if (!File.Exists(csvPath))
        {
            throw new FileNotFoundException("Файл CSV не знайдено!");
        }

        using var reader = new StreamReader(csvPath);
        using var csv = new CsvReader(
            reader, 
            CultureInfo.InvariantCulture);

        var allRecords = csv
            .GetRecords<dynamic>()
            .ToList();

        var records = allRecords.Where(r =>
            !string.IsNullOrWhiteSpace((string)r.Address) &&
            !string.IsNullOrWhiteSpace((string)r.Suburb) &&
            !string.IsNullOrWhiteSpace((string)r.Regionname) &&
            !string.IsNullOrWhiteSpace((string)r.Type) &&
            !string.IsNullOrWhiteSpace((string)r.Method) &&
            !string.IsNullOrWhiteSpace((string)r.Date) &&
            !string.IsNullOrWhiteSpace((string)r.Price) &&
            decimal.TryParse((string)r.Price, out decimal priceVal) && 
            priceVal > 0
        ).ToList();

        using var conn = new NpgsqlConnection(_targetDbConnString);
        conn.Open();

        var regions = records
            .Select(r => (string)r.Regionname)
            .Distinct();
        foreach (var r in regions)
        {
            conn
                .Execute(@"
                    INSERT INTO ""region"" (
                        ""regionName"") 
                    VALUES (@r) 
                    ON CONFLICT DO NOTHING", 
                    new { r });
        }

        var typeMapping = new Dictionary<string, string> 
        { 
            { "h", "House" }, 
            { "u", "Unit" }, 
            { "t", "Townhouse" }
        };

        var types = records
            .Select(r => (string)r.Type)
            .Distinct();

        foreach (var typeCode in types)
        {
            string typeName = typeMapping.ContainsKey(typeCode) ? 
                typeMapping[typeCode] : "Unknown";
            conn
                .Execute(@"
                    INSERT INTO ""buildingType"" (
                        ""buildingTypeCode"", 
                        ""buildingTypeName"") 
                    VALUES (
                        @Code, 
                        @Name) 
                    ON CONFLICT DO NOTHING", 
                    new 
                    { 
                        Code = typeCode, 
                        Name = typeName 
                    });
        }

        var methodMapping = new Dictionary<string, string> 
        { 
            { "S", "Property sold" }, 
            { "SP", "Property sold prior" }, 
            { "PI", "Property passed in" }, 
            { "VB", "Vendor bid" }, 
            { "SA", "Sold after auction" } 
        };

        var methods = records
            .Select(r => (string)r.Method)
            .Distinct();

        foreach (var methodCode in methods)
        {
            string methodName = methodMapping.ContainsKey(methodCode) ? 
                methodMapping[methodCode] : "Unknown";
            conn.Execute(@"
                INSERT INTO ""saleMethod"" (
                    ""saleMethodCode"", 
                    ""saleMethodName"") 
                VALUES (
                    @Code, 
                    @Name) 
                ON CONFLICT DO NOTHING", 
                new 
                { 
                    Code = methodCode, 
                    Name = methodName 
                });
        }

        var locations = records
            .GroupBy(r => r.Suburb)
            .Select(g => g.First());

        foreach (var loc in locations)
        {
            conn
                .Execute(@"
                    INSERT INTO ""location"" (
                        ""suburbName"", 
                        ""postCode"", 
                        ""totalPropertiesInSuburb"", 
                        ""regionId"")
                    SELECT 
                        @Suburb, 
                        @Postcode, 
                        CAST(NULLIF(@Propertycount, '') AS INTEGER), 
                        ""regionId"" 
                    FROM 
                        ""region"" 
                    WHERE 
                        ""regionName"" = @Regionname 
                    ON CONFLICT DO NOTHING",
                    new 
                    { 
                        Suburb = (string)loc.Suburb, 
                        Postcode = (string)loc.Postcode.Replace(".0", ""), 
                        Propertycount = (string)loc.Propertycount.Replace(".0", ""), 
                        Regionname = (string)loc.Regionname 
                    });
        }

        using var transaction = conn.BeginTransaction();
        try
        {
            foreach (var row in records)
            {
                var bId = conn.QuerySingle<int>(@"
                    INSERT INTO ""building"" (
                        ""streetAdress"",
                        ""totalRooms"",
                        ""distanceToCbd"",
                        ""bedroomCount"",
                        ""bathroomCount"",
                        ""parkingSpots"",
                        ""landAreaSqm"",
                        ""livingAreaSqm"",
                        ""yearBuilt"",
                        ""geoLattitude"",
                        ""geoLongtitude"",
                        ""buildingTypeId"",
                        ""locationId"")
                    SELECT
                        @Address,
                        CAST(NULLIF(@Rooms, '') AS SMALLINT),
                        CAST(NULLIF(@Distance, '') AS FLOAT),
                        CAST(NULLIF(@Bedroom2, '') AS SMALLINT),
                        CAST(NULLIF(@Bathroom, '') AS SMALLINT),
                        CAST(NULLIF(@Car, '') AS SMALLINT),
                        CAST(NULLIF(@Landsize, '') AS FLOAT),
                        CAST(NULLIF(@BuildingArea, '') AS FLOAT),
                        CAST(NULLIF(@YearBuilt, '') AS SMALLINT),
                        CAST(NULLIF(@Lattitude, '') AS FLOAT),
                        CAST(NULLIF(@Longtitude, '') AS FLOAT),
                        bt.""buildingTypeId"",
                        loc.""locationId""
                    FROM
                        ""buildingType"" bt,
                        ""location"" loc
                    WHERE
                        bt.""buildingTypeCode"" = @Type AND
                        loc.""suburbName"" = @Suburb
                    RETURNING ""buildingId""",
                    new 
                    { 
                        Address = (string)row.Address, 
                        Rooms = (string)row.Rooms, 
                        Distance = (string)row.Distance, 
                        Bedroom2 = (string)row.Bedroom2.Replace(".0", ""), 
                        Bathroom = (string)row.Bathroom.Replace(".0", ""), 
                        Car = (string)row.Car.Replace(".0", ""), 
                        Landsize = (string)row.Landsize, 
                        BuildingArea = (string)row.BuildingArea, 
                        YearBuilt = (string)row.YearBuilt.Replace(".0", ""), 
                        Lattitude = (string)row.Lattitude, 
                        Longtitude = (string)row.Longtitude, 
                        Type = (string)row.Type,
                        Suburb = (string)row.Suburb 
                    },
                    transaction: transaction);

                conn.Execute(@"
                    INSERT INTO ""sale"" (
                        ""salePrice"", 
                        ""saleDate"", 
                        ""buildingId"", 
                        ""saleMethodId"")
                    SELECT 
                        CAST(NULLIF(@Price, '') AS DECIMAL), 
                        TO_DATE(@Date, 'DD/MM/YYYY'), 
                        @bId, 
                        sm.""saleMethodId""
                    FROM 
                        ""saleMethod"" sm 
                    WHERE 
                        sm.""saleMethodCode"" = @Method",
                new 
                { 
                    Price = (string)row.Price, 
                    Date = (string)row.Date, 
                    bId = bId, 
                    Method = (string)row.Method 
                },
                transaction: transaction);
            }

            transaction.Commit();
        }
        catch (Exception ex)
        {
            transaction.Rollback();
            Console.WriteLine($"Помилка під час масового запису. Усі зміни скасовано. Деталі: " +
                $"{ex.Message}");
            throw;
        }
    }
}