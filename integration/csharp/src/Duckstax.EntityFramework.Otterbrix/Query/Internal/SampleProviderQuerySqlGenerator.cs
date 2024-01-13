namespace Duckstax.EntityFramework.Otterbrix.Query.Internal;

using System.Linq.Expressions;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
using Microsoft.EntityFrameworkCore.Sqlite.Query.Internal;

public class SampleProviderQuerySqlGenerator : QuerySqlGenerator
{
    private class SqliteQuerySqlGeneratorDerived : SqliteQuerySqlGenerator
    {
        public SqliteQuerySqlGeneratorDerived(QuerySqlGeneratorDependencies dependencies) : base(dependencies) { }

        public Expression VisitExtension_Exposed(Expression extensionExpression) => VisitExtension(extensionExpression);
        public string GetOperator_Exposed(SqlBinaryExpression binaryExpression) => GetOperator(binaryExpression);
        public void GenerateLimitOffset_Exposed(SelectExpression selectExpression) => GenerateLimitOffset(selectExpression);
        public void GenerateSetOperationOperand_Exposed(SetOperationBase setOperation, SelectExpression operand) => GenerateSetOperationOperand(setOperation, operand);
    }

    private readonly SqliteQuerySqlGeneratorDerived sqliteQuerySqlGeneratorFactory;

    public SampleProviderQuerySqlGenerator(QuerySqlGeneratorDependencies dependencies)
        : base(dependencies)
    {
        sqliteQuerySqlGeneratorFactory = new(dependencies);
    }

    protected override Expression VisitExtension(Expression extensionExpression) => sqliteQuerySqlGeneratorFactory.VisitExtension_Exposed(extensionExpression);
    protected override string GetOperator(SqlBinaryExpression binaryExpression) => sqliteQuerySqlGeneratorFactory.GetOperator_Exposed(binaryExpression);
    protected override void GenerateLimitOffset(SelectExpression selectExpression) => sqliteQuerySqlGeneratorFactory.GenerateLimitOffset_Exposed(selectExpression);
    protected override void GenerateSetOperationOperand(SetOperationBase setOperation, SelectExpression operand) 
        => sqliteQuerySqlGeneratorFactory.GenerateSetOperationOperand_Exposed(setOperation, operand);
}
