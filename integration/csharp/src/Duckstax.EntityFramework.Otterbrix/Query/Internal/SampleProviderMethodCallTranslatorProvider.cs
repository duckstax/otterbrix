namespace Duckstax.EntityFramework.Otterbrix.Query.Internal;

using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Sqlite.Query.Internal;

public class SampleProviderMethodCallTranslatorProvider : RelationalMethodCallTranslatorProvider
{
    public SampleProviderMethodCallTranslatorProvider(RelationalMethodCallTranslatorProviderDependencies dependencies)
    : base(dependencies)
    {
        //TODO: Replace the logic in this ctor.  All was taken from https://github.dev/dotnet/efcore/blob/38f69c6c9a773e3395f507d7e054f40d55962ced/src/EFCore.Sqlite.Core/Query/Internal/SqliteMethodCallTranslatorProvider.cs#L23-L40
        //      Copying was necessary since SqliteMethodCallTranslatorProvider's ctor casts dependencies.SqlExpressionFactory to a SqliteSqlExpressionFactory, but ours is a SampleProviderSqlExpressionFactory
        var sqlExpressionFactory = ((SampleProviderSqlExpressionFactory)dependencies.SqlExpressionFactory).SqliteSqlExpressionFactory;

        this.AddTranslators(
            new IMethodCallTranslator[]
            {
                new SqliteByteArrayMethodTranslator(sqlExpressionFactory),
                new SqliteCharMethodTranslator(sqlExpressionFactory),
                new SqliteDateTimeAddTranslator(sqlExpressionFactory),
                new SqliteGlobMethodTranslator(sqlExpressionFactory),
                new SqliteHexMethodTranslator(sqlExpressionFactory),
                new SqliteMathTranslator(sqlExpressionFactory),
                new SqliteObjectToStringTranslator(sqlExpressionFactory),
                new SqliteRandomTranslator(sqlExpressionFactory),
                new SqliteRegexMethodTranslator(sqlExpressionFactory),
                new SqliteStringMethodTranslator(sqlExpressionFactory),
                new SqliteSubstrMethodTranslator(sqlExpressionFactory)
            });

    }
}
