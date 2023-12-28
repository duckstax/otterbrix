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
        {
            string query = "INSERT INTO TestDatabase.TestCollection (_id, name, count) VALUES ";
                for (int num = 0; num < 100; ++num) {
                    query += ("('" + gen_id(num + 1) + "', "
                        + "'Name " + num + "', " + num + ")" + (num == 99 ? ";" : ", "));
                }
            cursorWrapper cursor = this.wrapper.Execute(query);
            Assert.IsTrue(cursor != null);
            Assert.IsTrue(cursor.IsSuccess());
            Assert.IsTrue(cursor.Size() == 100);
        }
        {
            string query = "SELECT * FROM TestDatabase.TestCollection;";
            cursorWrapper cursor = this.wrapper.Execute(query);
            Assert.IsTrue(cursor != null);
            Assert.IsTrue(cursor.IsSuccess());
            Assert.IsFalse(cursor.IsError());
            Assert.IsTrue(cursor.Size() == 100);
        }
        {
            string query = "SELECT * FROM TestDatabase.TestCollection WHERE count > 90;";
            cursorWrapper cursor = this.wrapper.Execute(query);
            Assert.IsTrue(cursor != null);
            Assert.IsTrue(cursor.IsSuccess());
            Assert.IsFalse(cursor.IsError());
            Assert.IsTrue(cursor.Size() == 9);
        }
        {
            string query = "SELECT * FROM TestDatabase.TestCollection ORDER BY count;";
            cursorWrapper cursor = this.wrapper.Execute(query);
            Assert.IsTrue(cursor != null);
            Assert.IsTrue(cursor.IsSuccess());
            Assert.IsFalse(cursor.IsError());
            Assert.IsTrue(cursor.Size() == 100);

            {
                documentWrapper doc = cursor.Next();
                Assert.IsTrue(doc != null);
                Assert.IsTrue(doc.GetLong("count") == 0);
                Assert.IsTrue(doc.GetString("name") == "Name 0");
            }
            {
                documentWrapper doc = cursor.Next();
                Assert.IsTrue(doc != null);
                Assert.IsTrue(doc.GetLong("count") == 1);
                Assert.IsTrue(doc.GetString("name") == "Name 1");
            }
            {
                documentWrapper doc = cursor.Next();
                Assert.IsTrue(doc != null);
                Assert.IsTrue(doc.GetLong("count") == 2);
                Assert.IsTrue(doc.GetString("name") == "Name 2");
            }
            {
                documentWrapper doc = cursor.Next();
                Assert.IsTrue(doc != null);
                Assert.IsTrue(doc.GetLong("count") == 3);
                Assert.IsTrue(doc.GetString("name") == "Name 3");
            }
            {
                documentWrapper doc = cursor.Next();
                Assert.IsTrue(doc != null);
                Assert.IsTrue(doc.GetLong("count") == 4);
                Assert.IsTrue(doc.GetString("name") == "Name 4");
            }
        }
    }
}