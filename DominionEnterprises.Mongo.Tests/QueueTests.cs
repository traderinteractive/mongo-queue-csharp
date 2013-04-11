using System;
using System.Configuration;
using System.Threading;
using NUnit.Framework;
using MongoDB.Bson;
using MongoDB.Driver;

namespace DominionEnterprises.Mongo.Tests
{
    [TestFixture]
    public class QueueTests
    {
        private MongoCollection collection;
        private Queue queue;

        [SetUp]
        public virtual void Setup()
        {
            collection = new MongoClient(ConfigurationManager.AppSettings["mongoQueueUrl"])
                .GetServer()
                .GetDatabase(ConfigurationManager.AppSettings["mongoQueueDb"])
                .GetCollection(ConfigurationManager.AppSettings["mongoQueueCollection"]);

            collection.Drop();

            queue = new Queue();
        }

        #region construct
        [Test]
        [ExpectedException(typeof(ArgumentNullException))]
        public void ConstructWithNullUrl()
        {
            new Queue(null, string.Empty, string.Empty);
        }

        [Test]
        [ExpectedException(typeof(ArgumentNullException))]
        public void ConstructWithNullDb()
        {
            new Queue(string.Empty, null, string.Empty);
        }

        [Test]
        [ExpectedException(typeof(ArgumentNullException))]
        public void ConstructWithNullCollection()
        {
            new Queue(string.Empty, string.Empty, null);
        }
        #endregion

        #region EnsureGetIndex
        [Test]
        public void EnsureGetIndex()
        {
            queue.EnsureGetIndex(new IndexKeysDocument("type", 1), new IndexKeysDocument("boo", -1));
            queue.EnsureGetIndex(new IndexKeysDocument("another.sub", 1));

            Assert.AreEqual(4, collection.GetIndexes().Count);

            var expectedOne = new IndexKeysDocument { { "running", 1 }, { "payload.type", 1 }, { "priority", 1 }, { "created", 1 }, { "payload.boo", -1 }, { "earliestGet", 1 } };
            Assert.AreEqual(expectedOne, collection.GetIndexes()[1].Key);

            var expectedTwo = new IndexKeysDocument { { "running", 1 }, { "resetTimestamp", 1 } };
            Assert.AreEqual(expectedTwo, collection.GetIndexes()[2].Key);

            var expectedThree = new IndexKeysDocument { { "running", 1 }, { "payload.another.sub", 1 }, { "priority", 1 }, { "created", 1 }, { "earliestGet", 1 } };
            Assert.AreEqual(expectedThree, collection.GetIndexes()[3].Key);
        }

        [Test]
        public void EnsureGetIndexWithNoArgs()
        {
            queue.EnsureGetIndex();

            Assert.AreEqual(3, collection.GetIndexes().Count);

            var expectedOne = new IndexKeysDocument { { "running", 1 }, { "priority", 1 }, { "created", 1 }, { "earliestGet", 1 } };
            Assert.AreEqual(expectedOne, collection.GetIndexes()[1].Key);

            var expectedTwo = new IndexKeysDocument { { "running", 1 }, { "resetTimestamp", 1 } };
            Assert.AreEqual(expectedTwo, collection.GetIndexes()[2].Key);
        }

        [Test]
        [ExpectedException(typeof(Exception))]
        public void EnsureGetIndexWithTooLongCollectionName()
        {
            //121 chars
            var collectionName = "messages01234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012";

            queue = new Queue(ConfigurationManager.AppSettings["mongoQueueUrl"], ConfigurationManager.AppSettings["mongoQueueDb"], collectionName);
            queue.EnsureGetIndex(new IndexKeysDocument());
        }

        [Test]
        [ExpectedException(typeof(ArgumentException))]
        public void EnsureGetIndexWithBadBeforeSortValue()
        {
            queue.EnsureGetIndex(new IndexKeysDocument("field", "NotAnInt"));
        }

        [Test]
        [ExpectedException(typeof(ArgumentException))]
        public void EnsureGetIndexWithBadAfterSortValue()
        {
            queue.EnsureGetIndex(new IndexKeysDocument(), new IndexKeysDocument("field", "NotAnInt"));
        }

        [Test]
        [ExpectedException(typeof(ArgumentNullException))]
        public void EnsureGetIndexWithNullBeforeSort()
        {
            queue.EnsureGetIndex(null, new IndexKeysDocument());
        }

        [Test]
        [ExpectedException(typeof(ArgumentNullException))]
        public void EnsureGetIndexWithNullAfterSort()
        {
            queue.EnsureGetIndex(new IndexKeysDocument(), null);
        }
        #endregion

        #region EnsureCountIndex
        [Test]
        public void EnsureCountIndex()
        {
            queue.EnsureCountIndex(new IndexKeysDocument { { "type", 1 }, { "boo", -1 } }, false);
            queue.EnsureCountIndex(new IndexKeysDocument { { "another.sub", 1 } }, true);

            Assert.AreEqual(3, collection.GetIndexes().Count);

            var expectedOne = new IndexKeysDocument { { "payload.type", 1 }, { "payload.boo", -1 } };
            Assert.AreEqual(expectedOne, collection.GetIndexes()[1].Key);

            var expectedTwo = new IndexKeysDocument { { "running", 1 }, { "payload.another.sub", 1 } };
            Assert.AreEqual(expectedTwo, collection.GetIndexes()[2].Key);
        }

        [Test]
        [ExpectedException(typeof(ArgumentException))]
        public void EnsureCountIndexWithBadValue()
        {
            queue.EnsureCountIndex(new IndexKeysDocument("field", "NotAnInt"), true);
        }

        [Test]
        [ExpectedException(typeof(ArgumentNullException))]
        public void EnsureCountIndexWithNull()
        {
            queue.EnsureCountIndex(null, true);
        }
        #endregion

        #region Get
        [Test]
        [ExpectedException(typeof(ArgumentNullException))]
        public void GetWithNullQuery()
        {
            queue.Get(null, TimeSpan.MaxValue);
        }

        [Test]
        public void GetByBadQuery()
        {
            queue.Send(new BsonDocument { { "key1", 0 }, { "key2", true } });

            var message = queue.Get(new QueryDocument { { "key1", 0 }, { "key2", false } }, TimeSpan.MaxValue, TimeSpan.Zero);
            Assert.IsNull(message);

            Assert.AreEqual(1, collection.Count());
        }

        [Test]
        public void GetByFullQuery()
        {
            var messageOne = new BsonDocument { { "id", "SHOULD BE REMOVED" }, { "key1", 0 }, { "key2", true } };

            queue.Send(messageOne);
            queue.Send(new BsonDocument("key", "value"));

            var result = queue.Get(new QueryDocument(messageOne), TimeSpan.FromHours(1), TimeSpan.MinValue);
            Assert.AreNotEqual(messageOne["id"], result["id"]);

            messageOne["id"] = result["id"];
            Assert.AreEqual(messageOne, result);
        }

        [Test]
        public void GetBySubDocQuery()
        {
            var messageTwo = new BsonDocument
            {
                {
                    "one",
                    new BsonDocument
                    {
                        { "two", new BsonDocument { { "three", 5 }, { "notused", "notused" } } },
                        { "notused", "notused" },
                    }
                },
                { "notused", "notused" },
            };

            queue.Send(new BsonDocument { { "key1", 0 }, { "key2", true } });
            queue.Send(messageTwo);

            var result = queue.Get(new QueryDocument("one.two.three", new BsonDocument("$gt", 4)), TimeSpan.MaxValue, TimeSpan.MaxValue);

            messageTwo.InsertAt(0, new BsonElement("id", result["id"]));
            Assert.AreEqual(messageTwo, result);
        }

        [Test]
        public void GetBeforeAck()
        {
            var messageOne = new BsonDocument { { "key1", 0 }, { "key2", true } };

            queue.Send(messageOne);
            queue.Send(new BsonDocument("key", "value"));

            queue.Get(new QueryDocument(messageOne), TimeSpan.MaxValue, TimeSpan.Zero);

            //try get message we already have before ack
            var result = queue.Get(new QueryDocument(messageOne), TimeSpan.MaxValue, TimeSpan.Zero);
            Assert.IsNull(result);
        }

        [Test]
        public void GetWithCustomPriority()
        {
            var messageOne = new BsonDocument { { "key", 0 } };
            var messageTwo = new BsonDocument { { "key", 1 } };
            var messageThree = new BsonDocument { { "key", 2 } };

            queue.Send(messageOne, DateTime.Now, 0.5);
            queue.Send(messageTwo, DateTime.Now, 0.4);
            queue.Send(messageThree, DateTime.Now, 0.3);

            var resultOne = queue.Get(new QueryDocument(), TimeSpan.MaxValue);
            var resultTwo = queue.Get(new QueryDocument(), TimeSpan.MaxValue);
            var resultThree = queue.Get(new QueryDocument(), TimeSpan.MaxValue);

            messageThree.InsertAt(0, new BsonElement("id", resultOne["id"]));
            messageTwo.InsertAt(0, new BsonElement("id", resultTwo["id"]));
            messageOne.InsertAt(0, new BsonElement("id", resultThree["id"]));

            Assert.AreEqual(messageThree, resultOne);
            Assert.AreEqual(messageTwo, resultTwo);
            Assert.AreEqual(messageOne, resultThree);
        }

        [Test]
        public void GetWithTimeBasedPriority()
        {
            var messageOne = new BsonDocument { { "key", 0 } };
            var messageTwo = new BsonDocument { { "key", 1 } };
            var messageThree = new BsonDocument { { "key", 2 } };

            queue.Send(messageOne);
            queue.Send(messageTwo);
            queue.Send(messageThree);

            var resultOne = queue.Get(new QueryDocument(), TimeSpan.MaxValue);
            var resultTwo = queue.Get(new QueryDocument(), TimeSpan.MaxValue);
            var resultThree = queue.Get(new QueryDocument(), TimeSpan.MaxValue);

            messageOne.InsertAt(0, new BsonElement("id", resultOne["id"]));
            messageTwo.InsertAt(0, new BsonElement("id", resultTwo["id"]));
            messageThree.InsertAt(0, new BsonElement("id", resultThree["id"]));

            Assert.AreEqual(messageOne, resultOne);
            Assert.AreEqual(messageTwo, resultTwo);
            Assert.AreEqual(messageThree, resultThree);
        }

        [Test]
        public void GetWait()
        {
            var start = DateTime.Now;

            queue.Get(new QueryDocument(), TimeSpan.MaxValue, TimeSpan.FromMilliseconds(200), TimeSpan.MinValue);

            var end = DateTime.Now;

            Assert.IsTrue(end - start >= TimeSpan.FromMilliseconds(200));
            Assert.IsTrue(end - start < TimeSpan.FromMilliseconds(400));
        }

        [Test]
        public void EarliestGet()
        {
            var messageOne = new BsonDocument { { "key1", 0 }, { "key2", true } };

            queue.Send(messageOne, DateTime.Now + TimeSpan.FromMilliseconds(200));

            var resultBefore = queue.Get(new QueryDocument(messageOne), TimeSpan.MaxValue, TimeSpan.Zero);
            Assert.IsNull(resultBefore);

            Thread.Sleep(200);

            var resultAfter = queue.Get(new QueryDocument(messageOne), TimeSpan.MaxValue, TimeSpan.Zero);
            Assert.IsNotNull(resultAfter);
        }
        #endregion

        [Test]
        public void ResetStuck()
        {
            var messageOne = new BsonDocument("key", 0);
            var messageTwo = new BsonDocument("key", 1);

            queue.Send(messageOne);
            queue.Send(messageTwo);

            //sets to running
            collection.Update(new QueryDocument("payload.key", 0), new UpdateDocument("$set", new BsonDocument { { "running", true }, { "resetTimestamp", DateTime.UtcNow } }));
            collection.Update(new QueryDocument("payload.key", 1), new UpdateDocument("$set", new BsonDocument { { "running", true }, { "resetTimestamp", DateTime.UtcNow } }));

            Assert.AreEqual(2, collection.Count(new QueryDocument("running", true)));

            //sets resetTimestamp on messageOne
            queue.Get(new QueryDocument(messageOne), TimeSpan.MinValue, TimeSpan.Zero);

            //resets and gets messageOne
            Assert.IsNotNull(queue.Get(new QueryDocument(messageOne), TimeSpan.MaxValue, TimeSpan.Zero));

            Assert.AreEqual(1, collection.Count(new QueryDocument("running", false)));
        }

        #region Count
        [Test]
        public void Count()
        {
            var message = new BsonDocument("boo", "scary");

            Assert.AreEqual(0, queue.Count(new QueryDocument(message), true));
            Assert.AreEqual(0, queue.Count(new QueryDocument(message), false));
            Assert.AreEqual(0, queue.Count(new QueryDocument(message)));

            queue.Send(message);
            Assert.AreEqual(1, queue.Count(new QueryDocument(message), false));
            Assert.AreEqual(0, queue.Count(new QueryDocument(message), true));
            Assert.AreEqual(1, queue.Count(new QueryDocument(message)));

            queue.Get(new QueryDocument(message), TimeSpan.MaxValue, TimeSpan.Zero);
            Assert.AreEqual(0, queue.Count(new QueryDocument(message), false));
            Assert.AreEqual(1, queue.Count(new QueryDocument(message), true));
            Assert.AreEqual(1, queue.Count(new QueryDocument(message)));
        }

        [Test]
        [ExpectedException(typeof(ArgumentNullException))]
        public void CountWithAbsentRunningAndNullQuery()
        {
            queue.Count(null);
        }

        [Test]
        [ExpectedException(typeof(ArgumentNullException))]
        public void CountWithRunningAndNullQuery()
        {
            queue.Count(null, true);
        }
        #endregion

        #region Ack
        [Test]
        public void Ack()
        {
            var messageOne = new BsonDocument { { "key1", 0 }, { "key2", true } };

            queue.Send(messageOne);
            queue.Send(new BsonDocument("key", "value"));

            var result = queue.Get(new QueryDocument(messageOne), TimeSpan.MaxValue, TimeSpan.Zero);
            Assert.AreEqual(2, collection.Count());

            queue.Ack(result);
            Assert.AreEqual(1, collection.Count());
        }

        [Test]
        [ExpectedException(typeof(ArgumentException))]
        public void AckWithWrongIdType()
        {
            queue.Ack(new BsonDocument("id", false));
        }

        [Test]
        [ExpectedException(typeof(ArgumentNullException))]
        public void AckWithNullMessage()
        {
            queue.Ack(null);
        }
        #endregion

        #region AckSend
        [Test]
        public void AckSend()
        {
            var messageOne = new BsonDocument { { "key1", 0 }, { "key2", true } };
            var messageThree = new BsonDocument { { "hi", "there" }, { "rawr", 2 } };

            queue.Send(messageOne);
            queue.Send(new BsonDocument("key", "value"));

            var resultOne = queue.Get(new QueryDocument(messageOne), TimeSpan.MaxValue, TimeSpan.Zero);
            Assert.AreEqual(2, collection.Count());

            queue.AckSend(resultOne, messageThree);
            Assert.AreEqual(2, collection.Count());

            var actual = queue.Get(new QueryDocument("hi", "there"), TimeSpan.MaxValue, TimeSpan.Zero);
            messageThree.InsertAt(0, new BsonElement("id", resultOne["id"]));
            Assert.AreEqual(messageThree, actual);
        }

        [Test()]
        [ExpectedException(typeof(ArgumentException))]
        public void AckSendWithWrongIdType()
        {
            queue.AckSend(new BsonDocument("id", 5), new BsonDocument("key", "value"));
        }

        [Test]
        [ExpectedException(typeof(ArgumentException))]
        public void AckSendWitNanPriority()
        {
            queue.AckSend(new BsonDocument("id", BsonObjectId.GenerateNewId()), new BsonDocument("key", "value"), DateTime.Now, Double.NaN);
        }

        [Test]
        [ExpectedException(typeof(ArgumentNullException))]
        public void AckSendWithNullMessage()
        {
            queue.AckSend(null, new BsonDocument("key", "value"));
        }

        [Test]
        [ExpectedException(typeof(ArgumentNullException))]
        public void AckSendWithNullPayload()
        {
            queue.AckSend(new BsonDocument("id", BsonObjectId.GenerateNewId()), null);
        }
        #endregion

        #region Requeue
        [Test]
        public void Requeue()
        {
            var messageOne = new BsonDocument { { "key1", 0 }, { "key2", true } };

            queue.Send(messageOne);
            queue.Send(new BsonDocument("key", "value"));

            var resultBeforeRequeue = queue.Get(new QueryDocument(messageOne), TimeSpan.MaxValue, TimeSpan.Zero);

            queue.Requeue(resultBeforeRequeue);
            Assert.AreEqual(2, collection.Count());

            var resultAfterRequeue = collection.FindOneAs<BsonDocument>(new QueryDocument("_id", resultBeforeRequeue["id"]));
            resultBeforeRequeue.Remove("id");
            Assert.AreEqual(resultBeforeRequeue, resultAfterRequeue["payload"]);
            Assert.AreEqual(2, collection.Count());
        }

        [Test]
        [ExpectedException(typeof(ArgumentException))]
        public void RequeueWithWrongIdType()
        {
            queue.Requeue(new BsonDocument("id", new BsonDocument()));
        }

        [Test]
        [ExpectedException(typeof(ArgumentNullException))]
        public void RequeueWithNullMessage()
        {
            queue.Requeue(null);
        }
        #endregion

        #region Send
        [Test]
        public void Send()
        {
            var now = DateTime.Now;

            var payload = new BsonDocument { { "key1", 0 }, { "key2", true } };
            queue.Send(payload, now, 0.8);

            var expected = new BsonDocument
            {
                //_id added below
                { "payload", payload },
                { "running", false },
                { "resetTimestamp", new BsonDateTime(DateTime.MaxValue) },
                { "earliestGet", new BsonDateTime(now) },
                { "priority", 0.8 },
                //created added below
            };

            var message = collection.FindOneAs<BsonDocument>();

            var actualCreated = message["created"];
            expected["created"] = actualCreated;
            actualCreated = actualCreated.AsDateTime;

            Assert.IsTrue(actualCreated <= DateTime.UtcNow);
            Assert.IsTrue(actualCreated > DateTime.UtcNow - TimeSpan.FromSeconds(10));

            expected.InsertAt(0, new BsonElement("_id", message["_id"]));
            Assert.AreEqual(expected, message);
        }

        [Test]
        [ExpectedException(typeof(ArgumentException))]
        public void SendWitNanPriority()
        {
            queue.Send(new BsonDocument("key", "value"), DateTime.Now, Double.NaN);
        }

        [Test]
        [ExpectedException(typeof(ArgumentNullException))]
        public void SendWithNullMessage()
        {
            queue.Send(null);
        }
        #endregion
    }
}
