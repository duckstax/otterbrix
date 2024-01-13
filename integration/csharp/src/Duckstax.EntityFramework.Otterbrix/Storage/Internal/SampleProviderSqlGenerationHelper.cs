namespace Duckstax.EntityFramework.Otterbrix.Storage.Internal;

using System.Text;
using Microsoft.EntityFrameworkCore.Sqlite.Storage.Internal;
using Microsoft.EntityFrameworkCore.Storage;

public class SampleProviderSqlGenerationHelper : RelationalSqlGenerationHelper
{
    private readonly SqliteSqlGenerationHelper sqliteSqlGenerationHelper;

    public SampleProviderSqlGenerationHelper(RelationalSqlGenerationHelperDependencies dependencies)
    : base(dependencies) 
    { 
        sqliteSqlGenerationHelper = new(dependencies);
    }

    public override string StartTransactionStatement => sqliteSqlGenerationHelper.StartTransactionStatement;

    public override string DelimitIdentifier(string name, string? schema) => sqliteSqlGenerationHelper.DelimitIdentifier(name, schema);

    public override void DelimitIdentifier(StringBuilder builder, string name, string? schema) => sqliteSqlGenerationHelper.DelimitIdentifier(builder, name, schema);
}
