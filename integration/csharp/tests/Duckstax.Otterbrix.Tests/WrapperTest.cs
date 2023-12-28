namespace Duckstax.Otterbrix.Tests;

using Duckstax.EntityFramework.Otterbrix;

public class Tests
{
    private otterbrixWrapper wrapper { get; set; }
    private string gen_id(int num)
    {
        string res = num.ToString();
        while (res.Length < 24) {
            res = "0" + res;
        }
        return res;
    }

    [SetUp]
    public void Setup()
    {
        this.wrapper = new otterbrixWrapper();
    }
    [Test]
    public void Test1()
    {

        cursorWrapper cursor = this.wrapper.Execute("CREATE DATABASE TestDatabase;");
        Assert.IsTrue(cursor != null);
        Assert.IsTrue(cursor.IsSuccess());
        //cursor = this.wrapper.Execute("CREATE COLLECTION TestDatabase TestCollection;");
        //Assert.IsTrue(cursor != null); 
        //Assert.IsTrue(cursor.IsSuccess());
        string query = "INSERT INTO TestDatabase.TestCollection (_id, name, count) VALUES ";
            for (int num = 0; num < 100; ++num) {
                query += ("('" + gen_id(num + 1) + "', "
                      + "'Name " + num + "', " + num + ")" + (num == 99 ? ";" : ", "));
            }
        cursor = this.wrapper.Execute(query);
        Assert.IsTrue(cursor != null);
        Assert.IsTrue(cursor.IsError());
    }
}