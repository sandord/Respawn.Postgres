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
        private const string RootConnString = "Server=127.0.0.1;Port=8099;User ID=docker;Password=Password12!;database=postgres";
        private const string DbConnString = "Server=127.0.0.1;Port=8099;User ID=docker;Password=Password12!;database={0}";

        private readonly ITestOutputHelper _output;
        private NpgsqlConnection _connection;
        private Database _database;
        private string _dbName;

        public PostgresTests(ITestOutputHelper output) => _output = output;

        public async Task InitializeAsync()
        {
            _dbName = DateTime.Now.ToString("yyyyMMddHHmmss") + Guid.NewGuid().ToString("N");

            using var connection = new NpgsqlConnection(RootConnString);
            connection.Open();

            using var cmd = connection.CreateCommand();
            cmd.CommandText = "create database \"" + _dbName + "\"";
            await cmd.ExecuteNonQueryAsync();

            _connection = new NpgsqlConnection(string.Format(DbConnString, _dbName));
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
        public async Task ShouldResetTwiceWithoutErrors()
        {
            var checkpoint = new PostgresCheckpoint(
                schemasToInclude:new[] { "public" }, 
                autoCreateExtensions: true
            );

            var connectionString = string.Format(DbConnString, _dbName);

            for (var i = 0; i < 2; i++)
            {
                await checkpoint.Reset(connectionString);
            }
        }
    }
}
