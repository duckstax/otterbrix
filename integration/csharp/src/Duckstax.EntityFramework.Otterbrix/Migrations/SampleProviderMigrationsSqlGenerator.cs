namespace Duckstax.EntityFramework.Otterbrix.Migrations;

using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.EntityFrameworkCore.Migrations.Operations;

public class SampleProviderMigrationsSqlGenerator : MigrationsSqlGenerator
{
    private class SqliteMigrationsSqlGeneratorDerived : SqliteMigrationsSqlGenerator
    {
        public SqliteMigrationsSqlGeneratorDerived(
            MigrationsSqlGeneratorDependencies dependencies,
            IRelationalAnnotationProvider migrationsAnnotations)
            : base(dependencies, migrationsAnnotations)
        {
        }

        public void Generate_Exposed(AlterDatabaseOperation operation, IModel? model, MigrationCommandListBuilder builder) => Generate(operation, model, builder);
        public void Generate_Exposed(AddColumnOperation operation, IModel? model, MigrationCommandListBuilder builder, bool terminate) => Generate(operation, model, builder, terminate);
        public void Generate_Exposed(DropIndexOperation operation, IModel? model, MigrationCommandListBuilder builder, bool terminate) => Generate(operation, model, builder, terminate);
        public void Generate_Exposed(RenameIndexOperation operation, IModel? model, MigrationCommandListBuilder builder) => Generate(operation, model, builder);
        public void Generate_Exposed(RenameTableOperation operation, IModel? model, MigrationCommandListBuilder builder) => Generate(operation, model, builder);
        public void Generate_Exposed(RenameColumnOperation operation, IModel? model, MigrationCommandListBuilder builder) => Generate(operation, model, builder);
        public void Generate_Exposed(CreateTableOperation operation, IModel? model, MigrationCommandListBuilder builder, bool terminate = true) => Generate(operation, model, builder, terminate);
        public void CreateTableColumns_Exposed(CreateTableOperation operation, IModel? model, MigrationCommandListBuilder builder) => CreateTableColumns(operation, model, builder);
        public void ColumnDefinition_Exposed(string? schema, string table, string name, ColumnOperation operation, IModel? model, MigrationCommandListBuilder builder) => ColumnDefinition(schema, table, name, operation, model, builder);
        public void Generate_Exposed(AddForeignKeyOperation operation, IModel? model, MigrationCommandListBuilder builder, bool terminate = true) => Generate(operation, model, builder, terminate);
        public void Generate_Exposed(AddPrimaryKeyOperation operation, IModel? model, MigrationCommandListBuilder builder, bool terminate = true) => Generate(operation, model, builder, terminate);
        public void Generate_Exposed(AddUniqueConstraintOperation operation, IModel? model, MigrationCommandListBuilder builder) => Generate(operation, model, builder);
        public void Generate_Exposed(AddCheckConstraintOperation operation, IModel? model, MigrationCommandListBuilder builder) => Generate(operation, model, builder);
        public void Generate_Exposed(DropColumnOperation operation, IModel? model, MigrationCommandListBuilder builder, bool terminate = true) => Generate(operation, model, builder, terminate);
        public void Generate_Exposed(DropForeignKeyOperation operation, IModel? model, MigrationCommandListBuilder builder, bool terminate = true) => Generate(operation, model, builder, terminate);
        public void Generate_Exposed(DropPrimaryKeyOperation operation, IModel? model, MigrationCommandListBuilder builder, bool terminate = true) => Generate(operation, model, builder, terminate);
        public void Generate_Exposed(DropUniqueConstraintOperation operation, IModel? model, MigrationCommandListBuilder builder) => Generate(operation, model, builder);
        public void Generate_Exposed(DropCheckConstraintOperation operation, IModel? model, MigrationCommandListBuilder builder) => Generate(operation, model, builder);
        public void Generate_Exposed(AlterColumnOperation operation, IModel? model, MigrationCommandListBuilder builder) => Generate(operation, model, builder);
        public void ComputedColumnDefinition_Exposed(string? schema, string table, string name, ColumnOperation operation, IModel? model, MigrationCommandListBuilder builder) => ComputedColumnDefinition(schema, table, name, operation, model, builder);
        public void Generate_Exposed(EnsureSchemaOperation operation, IModel? model, MigrationCommandListBuilder builder) => Generate(operation, model, builder);
        public void Generate_Exposed(DropSchemaOperation operation, IModel? model, MigrationCommandListBuilder builder) => Generate(operation, model, builder);
        public void Generate_Exposed(RestartSequenceOperation operation, IModel? model, MigrationCommandListBuilder builder) => Generate(operation, model, builder);
        public void Generate_Exposed(CreateSequenceOperation operation, IModel? model, MigrationCommandListBuilder builder) => Generate(operation, model, builder);
        public void Generate_Exposed(RenameSequenceOperation operation, IModel? model, MigrationCommandListBuilder builder) => Generate(operation, model, builder);
        public void Generate_Exposed(AlterSequenceOperation operation, IModel? model, MigrationCommandListBuilder builder) => Generate(operation, model, builder);
        public void Generate_Exposed(DropSequenceOperation operation, IModel? model, MigrationCommandListBuilder builder) => Generate(operation, model, builder);

    }

    private readonly SqliteMigrationsSqlGeneratorDerived sqliteMigrationsSqlGenerator;

    public SampleProviderMigrationsSqlGenerator(
        MigrationsSqlGeneratorDependencies dependencies,
        IRelationalAnnotationProvider migrationsAnnotations)
        : base(dependencies)
    {
        sqliteMigrationsSqlGenerator = new(dependencies, migrationsAnnotations);
    }

    public override IReadOnlyList<MigrationCommand> Generate(IReadOnlyList<MigrationOperation> operations, IModel? model = null, MigrationsSqlGenerationOptions options = MigrationsSqlGenerationOptions.Default)
        => sqliteMigrationsSqlGenerator.Generate(operations, model, options);

    protected override void Generate(AlterDatabaseOperation operation, IModel? model, MigrationCommandListBuilder builder)
        => sqliteMigrationsSqlGenerator.Generate_Exposed(operation, model, builder);

    protected override void Generate(AddColumnOperation operation, IModel? model, MigrationCommandListBuilder builder, bool terminate)
        => sqliteMigrationsSqlGenerator.Generate_Exposed(operation, model, builder, terminate);

    protected override void Generate(DropIndexOperation operation, IModel? model, MigrationCommandListBuilder builder, bool terminate)
        => sqliteMigrationsSqlGenerator.Generate_Exposed(operation, model, builder, terminate);

    protected override void Generate(RenameIndexOperation operation, IModel? model, MigrationCommandListBuilder builder)
        => sqliteMigrationsSqlGenerator.Generate_Exposed(operation, model, builder);

    protected override void Generate(RenameTableOperation operation, IModel? model, MigrationCommandListBuilder builder)
        => sqliteMigrationsSqlGenerator.Generate_Exposed(operation, model, builder);

    protected override void Generate(RenameColumnOperation operation, IModel? model, MigrationCommandListBuilder builder)
        => sqliteMigrationsSqlGenerator.Generate_Exposed(operation, model, builder);

    protected override void Generate(CreateTableOperation operation, IModel? model, MigrationCommandListBuilder builder, bool terminate = true)
        => sqliteMigrationsSqlGenerator.Generate_Exposed(operation, model, builder, terminate);

    protected override void CreateTableColumns(CreateTableOperation operation, IModel? model, MigrationCommandListBuilder builder)
        => sqliteMigrationsSqlGenerator.CreateTableColumns_Exposed(operation, model, builder);

    protected override void ColumnDefinition(string? schema, string table, string name, ColumnOperation operation, IModel? model, MigrationCommandListBuilder builder)
        => sqliteMigrationsSqlGenerator.ColumnDefinition_Exposed(schema, table, name, operation, model, builder);

    protected override void Generate(AddForeignKeyOperation operation, IModel? model, MigrationCommandListBuilder builder, bool terminate = true)
        => sqliteMigrationsSqlGenerator.Generate_Exposed(operation, model, builder, terminate);

    protected override void Generate(AddPrimaryKeyOperation operation, IModel? model, MigrationCommandListBuilder builder, bool terminate = true)
        => sqliteMigrationsSqlGenerator.Generate_Exposed(operation, model, builder, terminate);

    protected override void Generate(AddUniqueConstraintOperation operation, IModel? model, MigrationCommandListBuilder builder)
        => sqliteMigrationsSqlGenerator.Generate_Exposed(operation, model, builder);

    protected override void Generate(AddCheckConstraintOperation operation, IModel? model, MigrationCommandListBuilder builder)
        => sqliteMigrationsSqlGenerator.Generate_Exposed(operation, model, builder);

    protected override void Generate(DropColumnOperation operation, IModel? model, MigrationCommandListBuilder builder, bool terminate = true)
        => sqliteMigrationsSqlGenerator.Generate_Exposed(operation, model, builder, terminate);

    protected override void Generate(DropForeignKeyOperation operation, IModel? model, MigrationCommandListBuilder builder, bool terminate = true)
        => sqliteMigrationsSqlGenerator.Generate_Exposed(operation, model, builder, terminate);

    protected override void Generate(DropPrimaryKeyOperation operation, IModel? model, MigrationCommandListBuilder builder, bool terminate = true)
        => sqliteMigrationsSqlGenerator.Generate_Exposed(operation, model, builder, terminate);

    protected override void Generate(DropUniqueConstraintOperation operation, IModel? model, MigrationCommandListBuilder builder)
        => sqliteMigrationsSqlGenerator.Generate_Exposed(operation, model, builder);

    protected override void Generate(DropCheckConstraintOperation operation, IModel? model, MigrationCommandListBuilder builder)
        => sqliteMigrationsSqlGenerator.Generate_Exposed(operation, model, builder);

    protected override void Generate(AlterColumnOperation operation, IModel? model, MigrationCommandListBuilder builder)
        => sqliteMigrationsSqlGenerator.Generate_Exposed(operation, model, builder);

    protected override void ComputedColumnDefinition(string? schema, string table, string name, ColumnOperation operation, IModel? model, MigrationCommandListBuilder builder)
        => sqliteMigrationsSqlGenerator.ComputedColumnDefinition_Exposed(schema, table, name, operation, model, builder);

    protected override void Generate(EnsureSchemaOperation operation, IModel? model, MigrationCommandListBuilder builder)
        => sqliteMigrationsSqlGenerator.Generate_Exposed(operation, model, builder);

    protected override void Generate(DropSchemaOperation operation, IModel? model, MigrationCommandListBuilder builder)
        => sqliteMigrationsSqlGenerator.Generate_Exposed(operation, model, builder);

    protected override void Generate(RestartSequenceOperation operation, IModel? model, MigrationCommandListBuilder builder)
        => sqliteMigrationsSqlGenerator.Generate_Exposed(operation, model, builder);

    protected override void Generate(CreateSequenceOperation operation, IModel? model, MigrationCommandListBuilder builder)
        => sqliteMigrationsSqlGenerator.Generate_Exposed(operation, model, builder);

    protected override void Generate(RenameSequenceOperation operation, IModel? model, MigrationCommandListBuilder builder)
        => sqliteMigrationsSqlGenerator.Generate_Exposed(operation, model, builder);

    protected override void Generate(AlterSequenceOperation operation, IModel? model, MigrationCommandListBuilder builder)
        => sqliteMigrationsSqlGenerator.Generate_Exposed(operation, model, builder);

    protected override void Generate(DropSequenceOperation operation, IModel? model, MigrationCommandListBuilder builder)
        => sqliteMigrationsSqlGenerator.Generate_Exposed(operation, model, builder);
}
