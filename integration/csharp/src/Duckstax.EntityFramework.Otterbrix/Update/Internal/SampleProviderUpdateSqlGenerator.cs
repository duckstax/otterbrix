namespace Duckstax.EntityFramework.Otterbrix.Update.Internal;

using Microsoft.EntityFrameworkCore.Sqlite.Update.Internal;
using Microsoft.EntityFrameworkCore.Update;

public class SampleProviderUpdateSqlGenerator : UpdateSqlGenerator
{
    private readonly SqliteUpdateSqlGenerator sqliteUpdateSqlGenerator;

    public SampleProviderUpdateSqlGenerator(UpdateSqlGeneratorDependencies dependencies)
    : base(dependencies)
    {
        sqliteUpdateSqlGenerator = new(dependencies);
    }

    public override string GenerateNextSequenceValueOperation(string name, string? schema)
        => sqliteUpdateSqlGenerator.GenerateNextSequenceValueOperation(name, schema);
}
