namespace Duckstax.EntityFramework.Otterbrix.Storage.Internal;

using Microsoft.EntityFrameworkCore.Sqlite.Storage.Internal;
using Microsoft.EntityFrameworkCore.Storage;

public class SampleProviderTypeMappingSource : RelationalTypeMappingSource
{
    /// <summary>
    /// Derived class to expose protected methods of SQLite's SqliteTypeMappingSource class.
    /// </summary>
    private class SqliteTypeMappingSourceDerived : SqliteTypeMappingSource
    {
        public SqliteTypeMappingSourceDerived(TypeMappingSourceDependencies dependencies, RelationalTypeMappingSourceDependencies relationalDependencies)
            : base(dependencies, relationalDependencies)
        {

        }

        public RelationalTypeMapping? FindMapping_Exposed(in RelationalTypeMappingInfo mappingInfo)
            => FindMapping(mappingInfo);
    }

    private readonly SqliteTypeMappingSourceDerived sqliteTypeMappingSource;

    public SampleProviderTypeMappingSource(
		TypeMappingSourceDependencies dependencies,
        RelationalTypeMappingSourceDependencies relationalDependencies)
        : base(dependencies, relationalDependencies)
    {
        sqliteTypeMappingSource = new(dependencies, relationalDependencies);
    }

    //TODO:  Calling SQLite's logic to get the relational type mappings.
    protected override RelationalTypeMapping? FindMapping(in RelationalTypeMappingInfo mappingInfo)
        => sqliteTypeMappingSource.FindMapping_Exposed(mappingInfo);
}
