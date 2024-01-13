namespace Duckstax.EntityFramework.Otterbrix.Query.Internal;

using System.Linq.Expressions;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Sqlite.Query.Internal;

public class SampleProviderSqlTranslatingExpressionVisitor : RelationalSqlTranslatingExpressionVisitor
{
    private class SqliteSqlTranslatingExpressionVisitorDerived : SqliteSqlTranslatingExpressionVisitor
    {
        public SqliteSqlTranslatingExpressionVisitorDerived(
            RelationalSqlTranslatingExpressionVisitorDependencies dependencies,
            QueryCompilationContext queryCompilationContext,
            QueryableMethodTranslatingExpressionVisitor queryableMethodTranslatingExpressionVisitor)
            : base(dependencies, queryCompilationContext, queryableMethodTranslatingExpressionVisitor)
        {
        }

        public Expression VisitUnary_Exposed(UnaryExpression unaryExpression) => VisitUnary(unaryExpression);
        public Expression VisitBinary_Exposed(BinaryExpression binaryExpression) => VisitBinary(binaryExpression);
    }

    private readonly SqliteSqlTranslatingExpressionVisitorDerived sqliteSqlTranslatingExpressionVisitor;

    public SampleProviderSqlTranslatingExpressionVisitor(
        RelationalSqlTranslatingExpressionVisitorDependencies dependencies,
        QueryCompilationContext queryCompilationContext,
        QueryableMethodTranslatingExpressionVisitor queryableMethodTranslatingExpressionVisitor)
        : base(dependencies, queryCompilationContext, queryableMethodTranslatingExpressionVisitor)
    {
        sqliteSqlTranslatingExpressionVisitor = new(dependencies, queryCompilationContext, queryableMethodTranslatingExpressionVisitor);
    }

    protected override Expression VisitUnary(UnaryExpression unaryExpression) => sqliteSqlTranslatingExpressionVisitor.VisitUnary_Exposed(unaryExpression);

    protected override Expression VisitBinary(BinaryExpression binaryExpression) => sqliteSqlTranslatingExpressionVisitor.VisitBinary_Exposed(binaryExpression);
}
