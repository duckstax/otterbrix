namespace Duckstax.EntityFramework.otterbrix.Infrastructure;

using Duckstax.EntityFramework.otterbrix.Infrastructure.Internal;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;

public class SampleProviderDbContextOptionsBuilder : RelationalDbContextOptionsBuilder<SampleProviderDbContextOptionsBuilder, SampleProviderOptionsExtension>
{
    public SampleProviderDbContextOptionsBuilder(DbContextOptionsBuilder optionsBuilder)
        : base(optionsBuilder)
    {
    }
}
