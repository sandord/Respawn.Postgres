using System;
using System.Threading.Tasks;
using Npgsql;
using NPoco;
using Xunit;
using Xunit.Abstractions;

namespace Respawn.Postgres.IntegrationTests
{
    public class PostgresTests : IAsyncLifetime
    {
        private readonly ITestOutputHelper _output;
        private NpgsqlConnection _connection;
        private Database _database;

        public PostgresTests(ITestOutputHelper output) => _output = output;

        public async Task InitializeAsync()
        {
            var rootConnString = "Server=127.0.0.1;Port=8099;User ID=docker;Password=Password12!;database=postgres";
            var dbConnString = "Server=127.0.0.1;Port=8099;User ID=docker;Password=Password12!;database={0}";

            var dbName = DateTime.Now.ToString("yyyyMMddHHmmss") + Guid.NewGuid().ToString("N");
            using (var connection = new NpgsqlConnection(rootConnString))
            {
                connection.Open();

                using (var cmd = connection.CreateCommand())
                {
                    cmd.CommandText = "create database \"" + dbName + "\"";
                    await cmd.ExecuteNonQueryAsync();
                }
            }
            _connection = new NpgsqlConnection(string.Format(dbConnString, dbName));
            _connection.Open();

            _database = new Database(_connection, DatabaseType.PostgreSQL);
        }

        public Task DisposeAsync()
        {
            _connection?.Close();
            _connection?.Dispose();
            _connection = null;

            return Task.FromResult(0);
        }

        [SkipOnAppVeyor]
        public async Task ShouldCreateCacheDatabase()
        {
            // TODO:
        }
    }
}
