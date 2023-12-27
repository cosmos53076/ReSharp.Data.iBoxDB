using System.Collections.Generic;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace ReSharp.Data.IBoxDB.Tests
{
    public class TestObject
    {
        public TestObject()
        {
        }
        
        public TestObject(long id, string name)
        {
            Id = id;
            Name = name;
        }

        public long Id { get; set; }

        public string Name { get; set; }
    }

    public class DictionaryTestObject
    {
        public DictionaryTestObject()
        {
        }
        
        public DictionaryTestObject(long id, Dictionary<string, object> map)
        {
            Id = id;
            Map = map;
        }
        
        public long Id { get; set; }

        public Dictionary<string, object> Map { get; set; }
    }
    
    [TestClass]
    public class IBoxDBAdapterTests
    {
        [TestMethod]
        public void InsertTest()
        {
            const string tableName = nameof(TestObject);
            using var context = new IBoxDBAdapter();
            context.EnsureTable<TestObject>(tableName, "Id");
            context.Open();
            context.Insert(tableName, new TestObject(1L, "Test1"));
            var result = context.Get<TestObject>(tableName, 1L);
            Assert.AreEqual("Test1", result.Name);
        }

        [TestMethod]
        public void GetTest()
        {
            const string tableName = nameof(TestObject);
            using var context = new IBoxDBAdapter();
            context.EnsureTable<TestObject>(tableName, "Id");
            context.Open();
            context.Insert(tableName, new TestObject(1L, "Test1"));
            var result = context.Get<TestObject>(tableName, "Name", "Test1");
            Assert.AreEqual(1L, result[0].Id);
        }

        [TestMethod]
        public void GetAllTest()
        {
            const string tableName = nameof(TestObject);
            using var context = new IBoxDBAdapter();
            context.EnsureTable<TestObject>(tableName, "Id");
            context.Open();
            context.Insert(tableName, new TestObject(1L, "Test1"));
            context.Insert(tableName, new TestObject(2L, "Test2"));
            var result = context.GetAll<TestObject>(tableName);
            Assert.AreEqual(2, result.Count);
        }

        [TestMethod]
        public void DictionaryTest()
        {
            const string tableName = nameof(DictionaryTestObject);
            using var context = new IBoxDBAdapter();
            context.EnsureTable<DictionaryTestObject>(tableName, "Id");
            context.Open();
            var map = new Dictionary<string, object>
            {
                { "key1", 20 },
                { "key2", "key2" }
            };
            context.Insert(tableName, new DictionaryTestObject(1L, map));
            var result = context.Get<DictionaryTestObject>(tableName, 1L);
            var resultMap = result.Map;
            Assert.IsTrue(resultMap["key1"].Equals(map["key1"]) && resultMap["key2"].Equals(map["key2"]));
        }
    }
}