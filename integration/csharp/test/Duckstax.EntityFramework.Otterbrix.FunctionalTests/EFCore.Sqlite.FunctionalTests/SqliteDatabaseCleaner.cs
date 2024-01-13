//NOTE: This logic was copied from https://github.dev/dotnet/efcore/blob/38f69c6c9a773e3395f507d7e054f40d55962ced/test/EFCore.Sqlite.FunctionalTests/TestUtilities/SqliteDatabaseCleaner.cs#L12
//      because there was no other way to encorporate it for use by the Sample Provider's functional tests.

// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.

using System.Data;
using Microsoft.EntityFrameworkCore.Diagnostics;
using System.Diagnostics;
using Microsoft.EntityFrameworkCore.Diagnostics.Internal;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Scaffolding;
using Microsoft.EntityFrameworkCore.Scaffolding.Metadata;
using Microsoft.EntityFrameworkCore.Sqlite.Design.Internal;
using Microsoft.EntityFrameworkCore.Sqlite.Diagnostics.Internal;
using Microsoft.EntityFrameworkCore.Storage.ValueConversion;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Microsoft.EntityFrameworkCore.TestUtilities;

public class SqliteDatabaseCleaner : RelationalDatabaseCleaner
{
    protected override IDatabaseModelFactory CreateDatabaseModelFactory(ILoggerFactory loggerFactory)
    {
        // NOTE: You may need to update AddEntityFrameworkDesignTimeServices() too
        var services = new ServiceCollection()
            .AddSingleton<TypeMappingSourceDependencies>()
            .AddSingleton<RelationalTypeMappingSourceDependencies>()
            .AddSingleton<ValueConverterSelectorDependencies>()
            .AddSingleton<DiagnosticSource>(new DiagnosticListener(DbLoggerCategory.Name))
            .AddSingleton<ILoggingOptions, LoggingOptions>()
            .AddSingleton<IDbContextLogger, NullDbContextLogger>()
            .AddSingleton<LoggingDefinitions, SqliteLoggingDefinitions>()
            .AddSingleton(typeof(IDiagnosticsLogger<>), typeof(DiagnosticsLogger<>))
            .AddSingleton<IValueConverterSelector, ValueConverterSelector>()
            .AddSingleton<IInterceptors, Interceptors>()
            .AddLogging();
        new SqliteDesignTimeServices().ConfigureDesignTimeServices(services);

        return services
            .BuildServiceProvider() // No scope validation; cleaner violates scopes, but only resolve services once.
            .GetRequiredService<IDatabaseModelFactory>();
    }

    protected override bool AcceptForeignKey(DatabaseForeignKey foreignKey)
        => false;

    protected override bool AcceptIndex(DatabaseIndex index)
        => false;

    protected override string BuildCustomSql(DatabaseModel databaseModel)
        => "PRAGMA foreign_keys=OFF;";

    protected override string BuildCustomEndingSql(DatabaseModel databaseModel)
        => "PRAGMA foreign_keys=ON;";

    public override void Clean(DatabaseFacade facade)
    {
        var connection = facade.GetDbConnection();

        var opened = false;
        if (connection.State != ConnectionState.Open)
        {
            connection.Open();
            opened = true;
        }

        var command = connection.CreateCommand();
        command.CommandText = "SELECT COUNT(*) FROM sqlite_master WHERE name = 'geometry_columns' AND type = 'table';";

        var hasGeometryColumns = (long)command.ExecuteScalar() != 0L;
        if (hasGeometryColumns)
        {
            // NB: SUM forces DiscardGeometryColumn to evaluate for each row
            command.CommandText = "SELECT SUM(DiscardGeometryColumn(f_table_name, f_geometry_column)) FROM geometry_columns;";
            command.ExecuteNonQuery();
        }

        if (opened)
        {
            connection.Close();
        }

        base.Clean(facade);
    }
}
