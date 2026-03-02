using Microsoft.Extensions.Configuration;

public class Program
{
    public static void Main()
    {
        try
        {
            var config = new ConfigurationBuilder()
                .SetBasePath(Directory.GetCurrentDirectory())
                .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true)
                .Build();

            string serverConn = config.GetConnectionString("PostgresServer");
            string targetDbConn = config.GetConnectionString("MelbourneDb");
            string csvPath = config["DataSettings:CsvFilePath"];

            var dbSetupService = new DatabaseSetupService(serverConn, targetDbConn);
            dbSetupService.InitializeDatabase();

            var importService = new DataImportService(targetDbConn);
            importService.ImportFromCsv(csvPath);

            Console.WriteLine("\nПроцес завершено");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"\nПомилка: {ex.Message}");
        }
    }
}