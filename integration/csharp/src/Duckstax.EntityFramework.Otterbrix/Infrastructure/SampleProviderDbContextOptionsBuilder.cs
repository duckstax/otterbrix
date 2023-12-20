namespace Duckstax.EntityFramework.Otterbrix.Infrastructure;

using Duckstax.EntityFramework.Otterbrix.Infrastructure.Internal;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;

public class SampleProviderDbContextOptionsBuilder : RelationalDbContextOptionsBuilder<SampleProviderDbContextOptionsBuilder, SampleProviderOptionsExtension>
{
    public SampleProviderDbContextOptionsBuilder(DbContextOptionsBuilder optionsBuilder)
        : base(optionsBuilder)
    {
    }
}
