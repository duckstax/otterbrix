using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Scaffolding;
using Microsoft.EntityFrameworkCore.Scaffolding.Metadata;
using Microsoft.EntityFrameworkCore.TestUtilities;
using Microsoft.Extensions.Logging;

namespace Duckstax.EntityFramework.Otterbrix.FunctionalTests.TestUtilities;

public class SampleProviderDatabaseCleaner : RelationalDatabaseCleaner
{
    private class SqliteDatabaseCleanerDerived : SqliteDatabaseCleaner
    {
        public IDatabaseModelFactory CreateDatabaseModelFactory_Exposed(ILoggerFactory loggerFactory) => CreateDatabaseModelFactory(loggerFactory);
        public bool AcceptForeignKey_Exposed(DatabaseForeignKey foreignKey) => AcceptForeignKey(foreignKey);
        public bool AcceptIndex_Exposed(DatabaseIndex index) => AcceptIndex(index);
        public string BuildCustomSql_Exposed(DatabaseModel databaseModel) => BuildCustomSql(databaseModel);
        public string BuildCustomEndingSql_Exposed(DatabaseModel databaseModel) => BuildCustomEndingSql(databaseModel);
    }

    private readonly SqliteDatabaseCleanerDerived sqliteDatabaseCleaner = new();

    protected override IDatabaseModelFactory CreateDatabaseModelFactory(ILoggerFactory loggerFactory)
        => sqliteDatabaseCleaner.CreateDatabaseModelFactory_Exposed(loggerFactory);

    protected override bool AcceptForeignKey(DatabaseForeignKey foreignKey)
        => sqliteDatabaseCleaner.AcceptForeignKey_Exposed(foreignKey);

    protected override bool AcceptIndex(DatabaseIndex index)
        => sqliteDatabaseCleaner.AcceptIndex_Exposed(index);

    protected override string BuildCustomSql(DatabaseModel databaseModel)
        => sqliteDatabaseCleaner.BuildCustomSql_Exposed(databaseModel);

    protected override string BuildCustomEndingSql(DatabaseModel databaseModel)
        => sqliteDatabaseCleaner.BuildCustomEndingSql_Exposed(databaseModel);

    public override void Clean(DatabaseFacade facade)
        => sqliteDatabaseCleaner.Clean(facade);
}
