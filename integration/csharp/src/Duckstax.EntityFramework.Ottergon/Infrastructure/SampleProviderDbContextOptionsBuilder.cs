namespace Duckstax.EntityFramework.Ottergon.Infrastructure;

using Duckstax.EntityFramework.Ottergon.Infrastructure.Internal;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;

public class SampleProviderDbContextOptionsBuilder : RelationalDbContextOptionsBuilder<SampleProviderDbContextOptionsBuilder, SampleProviderOptionsExtension>
{
    public SampleProviderDbContextOptionsBuilder(DbContextOptionsBuilder optionsBuilder)
        : base(optionsBuilder)
    {
    }
}
