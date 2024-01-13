namespace Duckstax.EntityFramework.Otterbrix.Infrastructure.Internal;

using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Sqlite.Infrastructure.Internal;

public class SampleProviderModelValidator : RelationalModelValidator
{
    private class SqliteModelValidatorDerived : SqliteModelValidator
    {
        public SqliteModelValidatorDerived(ModelValidatorDependencies dependencies, RelationalModelValidatorDependencies relationalDependencies)
            : base(dependencies, relationalDependencies)
        {

        }

        public void ValidateCompatible_Exposed(
            IProperty property,
            IProperty duplicateProperty,
            string columnName,
            in StoreObjectIdentifier storeObject,
            IDiagnosticsLogger<DbLoggerCategory.Model.Validation> logger) 
                => ValidateCompatible(property, duplicateProperty, columnName, storeObject, logger);

        public void ValidateValueGeneration_Exposed(
            IEntityType entityType,
            IKey key,
            IDiagnosticsLogger<DbLoggerCategory.Model.Validation> logger) => ValidateValueGeneration(entityType, key, logger);
    }

    private readonly SqliteModelValidatorDerived sqliteModelValidator;

    public SampleProviderModelValidator(
        ModelValidatorDependencies dependencies,
        RelationalModelValidatorDependencies relationalDependencies)
        : base(dependencies, relationalDependencies)
    {
        sqliteModelValidator = new(dependencies, relationalDependencies);
    }

    public override void Validate(IModel model, IDiagnosticsLogger<DbLoggerCategory.Model.Validation> logger)
        => sqliteModelValidator.Validate(model, logger);

    protected override void ValidateCompatible(
        IProperty property,
        IProperty duplicateProperty,
        string columnName,
        in StoreObjectIdentifier storeObject,
        IDiagnosticsLogger<DbLoggerCategory.Model.Validation> logger) => sqliteModelValidator.ValidateCompatible_Exposed(property, duplicateProperty, columnName, storeObject, logger);

    protected override void ValidateValueGeneration(
        IEntityType entityType,
        IKey key,
        IDiagnosticsLogger<DbLoggerCategory.Model.Validation> logger) => sqliteModelValidator.ValidateValueGeneration_Exposed(entityType, key, logger);
}
