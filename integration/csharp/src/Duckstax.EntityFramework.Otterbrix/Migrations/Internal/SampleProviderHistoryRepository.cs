namespace Duckstax.EntityFramework.Otterbrix.Migrations.Internal;

using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.EntityFrameworkCore.Sqlite.Migrations.Internal;

public class SampleProviderHistoryRepository : HistoryRepository
{
    private class SqliteHistoryRepositoryDerived : SqliteHistoryRepository
    {
        public SqliteHistoryRepositoryDerived(HistoryRepositoryDependencies dependencies)
            : base(dependencies)
        {
        }

        public string ExistsSql_Exposed => ExistsSql;
        public bool InterpretExistsResult_Exposed(object? value) => InterpretExistsResult(value);
    }

    private readonly SqliteHistoryRepositoryDerived sqliteHistoryRepository;
    public SampleProviderHistoryRepository(HistoryRepositoryDependencies dependencies)
    : base(dependencies)
    {
        sqliteHistoryRepository = new(dependencies);
    }

    protected override string ExistsSql => sqliteHistoryRepository.ExistsSql_Exposed;
    protected override bool InterpretExistsResult(object? value) => sqliteHistoryRepository.InterpretExistsResult_Exposed(value);
    public override string GetCreateIfNotExistsScript() => sqliteHistoryRepository.GetCreateIfNotExistsScript();
    public override string GetBeginIfNotExistsScript(string migrationId) => sqliteHistoryRepository.GetBeginIfNotExistsScript(migrationId);
    public override string GetBeginIfExistsScript(string migrationId) => sqliteHistoryRepository.GetBeginIfExistsScript(migrationId);
    public override string GetEndIfScript() => sqliteHistoryRepository.GetEndIfScript();
}
