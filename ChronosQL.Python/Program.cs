using ChronosQL.Engine;

namespace ChronosQL.Python;

public static class Program
{
    public static async Task<int> Main(string[] args)
    {
        if (args.Length < 2)
        {
            Console.Error.WriteLine("Usage: ChronosQL.Python <sql> <json-lines>");
            return 1;
        }

        var sql = args[0];
        var jsonLines = args[1]
            .Split('\n', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);

        var engine = new ChronosQLEngine();
        var results = await engine.ExecuteBatchAsJsonAsync(sql, jsonLines);

        foreach (var line in results)
        {
            Console.WriteLine(line);
        }

        return 0;
    }
}
