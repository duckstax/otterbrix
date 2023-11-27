namespace Duckstax.Ottergon.Tests;

using Duckstax.EntityFramework.Ottergon;

public class Tests
{
    private OttergonWrapper wrapper { get; set; }

    [SetUp]
    public void Setup()
    {
        this.wrapper = new OttergonWrapper();
    }

    [Test]
    public void Test1()
    {
        var result = this.wrapper.Execute();
        Assert.IsTrue(result);
    }
}