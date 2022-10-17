// See https://aka.ms/new-console-template for more information

using Npgsql;

Console.WriteLine("Hello, World!");

var skus = GenerateTestData();

var token = new CancellationTokenSource().Token;
var batch_size = 100_000;

Console.WriteLine($"Insert by batches with size = {batch_size}");
var counter = 0;
foreach (var batch in skus.Chunk(batch_size))
{
    await BulkInsert(batch, token);
    counter += batch_size;
    Console.WriteLine($"Inserted items: {counter}");
}


Sku[] GenerateTestData() =>
    Enumerable.Range(1, 10_000_000)
        .Select(x => new Sku {Id = x, Name = $"Sku_{x}"})
        .ToArray();

async Task BulkInsert(Sku[] itemsToInsert, CancellationToken token)
{
    var connectionString =
        "Server=localhost;Port=5432;Database=postgres;User ID=postgres;Password=pwd;No Reset On Close=true";
    using var connection = new NpgsqlConnection(connectionString);
    await connection.OpenAsync(token);
    await using var transaction = await connection.BeginTransactionAsync(token);

    #region Типо миграция, создаем таблицу в которую будем наливать

    const string CreateTargetTable = @"
CREATE TABLE IF NOT EXISTS items
(
    id   bigint   NOT NULL,
    name text     NOT NULL
);";

    await using var targetTableCommand = new NpgsqlCommand(CreateTargetTable, connection);
    await targetTableCommand.ExecuteNonQueryAsync(token);

    #endregion

    const string TempTableQuery = @"
CREATE TEMP TABLE t_items
(
    id   bigint   NOT NULL,
    name text     NOT NULL
) ON COMMIT DROP;";

    const string UpsertQuery = @"
INSERT INTO items (id, name)
SELECT
       id,
       name
FROM t_items";

    const string BulkCopyQuery = "COPY t_items FROM STDIN (FORMAT BINARY);";

    await using var tempTableCommand = new NpgsqlCommand(TempTableQuery, connection);
    await tempTableCommand.ExecuteNonQueryAsync(token);

    await using (var importer = await connection.BeginBinaryImportAsync(BulkCopyQuery, token))
    {
        foreach (var sku in itemsToInsert)
        {
            await importer.StartRowAsync(token);
            await importer.WriteAsync(sku.Id, token);
            await importer.WriteAsync(sku.Name, token);
        }

        await importer.CompleteAsync(token);
    }

    await using var command = new NpgsqlCommand(UpsertQuery, connection);
    await command.ExecuteNonQueryAsync(token);

    await transaction.CommitAsync(token);
}