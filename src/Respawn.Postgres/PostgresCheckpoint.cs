using System;
using System.Threading.Tasks;
using Npgsql;

namespace Respawn.Postgres
{
    public class PostgresCheckpoint
    {
        public const string CacheDatabasePostfix = "__respawn_cache";

        private readonly Lazy<Checkpoint> _checkpoint;

        public string[] TablesToIgnore
        {
            get => _checkpoint.Value.TablesToIgnore;
            set => _checkpoint.Value.TablesToIgnore = value;
        }

        public string[] SchemasToInclude
        {
            get => _checkpoint.Value.SchemasToInclude;
            set => _checkpoint.Value.SchemasToInclude = value;
        }

        public string[] SchemasToExclude
        {
            get => _checkpoint.Value.SchemasToExclude;
            set => _checkpoint.Value.SchemasToExclude = value;
        }

        public int? CommandTimeout
        {
            get => _checkpoint.Value.CommandTimeout;
            set => _checkpoint.Value.CommandTimeout = value;
        }

        public bool AutoCreateExtensions { get; set; }

        public PostgresCheckpoint()
        {
            _checkpoint = new Lazy<Checkpoint>();
        }

        public async Task Reset(string connectionString)
        {
            if (string.IsNullOrEmpty(connectionString)) throw new ArgumentException("Null or empty.", nameof(connectionString));

            var databaseName = PostgresHelper.ExtractDatabaseName(connectionString);
            var cacheDatabaseName = databaseName + CacheDatabasePostfix;

            if (AutoCreateExtensions)
            {
                CreatePostgresExtensions(connectionString);
            }

            var cacheDatabaseExists = PostgresHelper.GetDatabaseExists(connectionString, cacheDatabaseName);
            var cacheDatabaseIsOutdated = !CheckCacheDatabaseIsUpToDate(connectionString, cacheDatabaseName);

            if (cacheDatabaseExists && !cacheDatabaseIsOutdated)
            {
                // Copy cache database onto main database.
                PostgresHelper.CopyDatabaseIfNotExists(connectionString, databaseName, cacheDatabaseName);
            }
            else
            {
                await _checkpoint.Value.Reset(connectionString);

                // Copy main database onto cache database.
                PostgresHelper.CopyDatabaseIfNotExists(connectionString, cacheDatabaseName);
            }

            // Clear the connection pools because there may be connections in them which were broken by a PostgresHelper.CloseClientConnections call.
            PostgresHelper.ClearAllPools();
        }

        private static bool CheckCacheDatabaseIsUpToDate(string connectionString, string cacheDatabaseName)
        {
            var connectionStringBuilder = new NpgsqlConnectionStringBuilder(connectionString)
            {
                Database = cacheDatabaseName
            };

            var cacheDatabaseConnectionString = connectionStringBuilder.ConnectionString;

            var databaseStructureHash = PostgresHelper.GetDatabaseStructureHash(connectionString);
            var cacheDatabaseStructureHash = PostgresHelper.GetDatabaseStructureHash(cacheDatabaseConnectionString);

            return databaseStructureHash == cacheDatabaseStructureHash;
        }

        private static void CreatePostgresExtensions(string connectionString)
        {
            PostgresHelper.CreateExtensionIfNotExists(connectionString, "dblink");
        }
    }
}
