# Respawn.Postgres
Builds upon Jimmy Bogard's [Respawn](https://github.com/jbogard/Respawn) and provides enhanced performance for Postgres databases. Respawn is an intelligent database cleaner for integration tests.

## Introduction

Respawn is a small utility to help in resetting test databases to a clean state. Instead of deleting data at the end of a test or rolling back a transaction, Respawn [resets the database back to a clean checkpoint](http://lostechies.com/jimmybogard/2013/06/18/strategies-for-isolating-the-database-in-tests/) by intelligently deleting data from tables.

Please refer to [Respawn on GitHub](https://github.com/jbogard/Respawn) for more information about the innards of Respawn.

I was using Respawn a while ago to reset a Postgres database in a project of mine during integration testing. I was a little disappointed by its
performance though, seeing running times of around 13 seconds to clear a 52 table database that didn't even contain any data to begin with.

After doing some research, I concluded that the best approach would be using Postgres' `CREATE DATABASE dbname TEMPLATE template` (see [Template Databases in the PostgreSQL Manual](https://www.postgresql.org/docs/9.3/static/manage-ag-templatedbs.html)).

In my case, this reduced the time to reset the database to a clean state to less than a second, which is about 13 times faster! I have to note though that the first run still takes 13 seconds but subsequent runs take advantage of the cached database and like I said, that takes less than a second.

## Requirements

These are some requirements that apply in addition to vanilla Respawn.

- The `dblink` Postgres extension is required to be installed in your database. The `PostgresCheckpoint.AutoCreateExtensions` property can be set to `true` to automatically install the required extension(s).

## Implications

Please make sure you understand the following implications and then decide whether you still want to use this library.

- A cache version of your database will be created.
- Existing connections to either the database or the cached database may be dropped during the process. Any open connections in the ADO.NET connection pool will be corrupted as a result of this so the connection pool will be reset as well to counter that issue.

## Usage

To use, create a `PostgresCheckpoint` and initialize with tables you want to skip, or schemas you want to keep/ignore:

```csharp
private static PostgresCheckpoint checkpoint = new PostgresCheckpoint
{
    AutoCreateExtensions = true,

    SchemasToInclude = new[]
    {
        "public"
    }
};
```

In your tests, in the fixture setup, reset your checkpoint:

```csharp
await checkpoint.Reset("MyConnectionString");
```

## Installing Respawn.Postgres

You should install [Respawn.Postgres with NuGet](https://www.nuget.org/packages/Respawn.Postgres):

```powershell
Install-Package Respawn.Postgres
```

Or via the .NET Core CLI:

```powershell
dotnet add package Respawn.Postgres
```

This command from Package Manager Console will download and install Respawn.Postgres.

## Local development

To install and run local dependencies needed for integration tests (PostgreSQL), install Docker for Windows and from the command line at the solution root run:

```powershell
docker-compose up -d
```

This will pull down the latest container images and run them. You can then run the local build/tests.
