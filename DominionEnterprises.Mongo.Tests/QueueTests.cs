using System;
using System.Configuration;
using System.Threading;
using NUnit.Framework;
using MongoDB.Bson;
using MongoDB.Driver;
using MongoDB.Driver.GridFS;
using System.IO;
using System.Collections.Generic;

namespace DominionEnterprises.Mongo.Tests
{
    [TestFixture]
    public class QueueTests
    {
        private MongoCollection collection;
        private MongoGridFS gridfs;
        private Queue queue;

        [SetUp]
        public virtual void Setup()
        {
            collection = new MongoClient(ConfigurationManager.AppSettings["mongoQueueUrl"])
                .GetServer()
                .GetDatabase(ConfigurationManager.AppSettings["mongoQueueDb"])
                .GetCollection(ConfigurationManager.AppSettings["mongoQueueCollection"]);

            collection.Drop();

            gridfs = collection.Database.GetGridFS(MongoGridFSSettings.Defaults);
            gridfs.Files.Drop();
            gridfs.Chunks.Drop();

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

        [Test]
        [ExpectedException(typeof(ArgumentNullException))]
        public void ConstructWithNullCollectionObject()
        {
            new Queue(null);
        }

        [Test]
        public void ConstructCollectionObject()
        {
            var collection = new MongoClient(ConfigurationManager.AppSettings["mongoQueueUrl"])
                .GetServer()
                .GetDatabase(ConfigurationManager.AppSettings["mongoQueueDb"])
                .GetCollection(ConfigurationManager.AppSettings["mongoQueueCollection"]);
            new Queue(collection);
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
        public void EnsureCountIndexWithPrefixOfPrevious()
        {
            queue.EnsureCountIndex(new IndexKeysDocument { { "type", 1 }, { "boo", -1 } }, false);
            queue.EnsureCountIndex(new IndexKeysDocument { { "type", 1 } }, false);

            Assert.AreEqual(2, collection.GetIndexes().Count);

            var expectedOne = new IndexKeysDocument { { "payload.type", 1 }, { "payload.boo", -1 } };
            Assert.AreEqual(expectedOne, collection.GetIndexes()[1].Key);
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
            var messageOne = new BsonDocument { { "id", "SHOULD NOT BE AFFECTED" }, { "key1", 0 }, { "key2", true } };

            using (var streamOne = new MemoryStream())
            using (var streamTwo = new MemoryStream())
            {
                streamOne.WriteByte(111);
                streamTwo.WriteByte(222);
                streamOne.Position = 0;
                streamTwo.Position = 0;
                queue.Send(messageOne, DateTime.Now, 0.0, new Dictionary<string, Stream>{ { "one", streamOne }, { "two", streamTwo } });
            }
            queue.Send(new BsonDocument("key", "value"));

            var result = queue.Get(new QueryDocument(messageOne), TimeSpan.FromHours(1), TimeSpan.MinValue);

            Assert.AreEqual(messageOne, result.Payload);
            Assert.AreEqual(111, result.Streams["one"].ReadByte());
            Assert.AreEqual(222, result.Streams["two"].ReadByte());
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

            var result = queue.Get(new QueryDocument("one.two.three", new BsonDocument("$gt", 4)), TimeSpan.MaxValue, TimeSpan.MaxValue, TimeSpan.MinValue, false);

            Assert.AreEqual(messageTwo, result.Payload);
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

            Assert.AreEqual(messageThree, resultOne.Payload);
            Assert.AreEqual(messageTwo, resultTwo.Payload);
            Assert.AreEqual(messageOne, resultThree.Payload);
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

            Assert.AreEqual(messageOne, resultOne.Payload);
            Assert.AreEqual(messageTwo, resultTwo.Payload);
            Assert.AreEqual(messageThree, resultThree.Payload);
        }

        [Test]
        public void GetWithTimeBasedPriorityAndOldTimestamp()
        {
            var messageOne = new BsonDocument { { "key", 0 } };
            var messageTwo = new BsonDocument { { "key", 1 } };
            var messageThree = new BsonDocument { { "key", 2 } };

            queue.Send(messageOne);
            queue.Send(messageTwo);
            queue.Send(messageThree);

            var resultTwo = queue.Get(new QueryDocument(), TimeSpan.MaxValue);
            //ensuring using old timestamp shouldn't affect normal time order of Send()s
            queue.AckSend(resultTwo.Handle, resultTwo.Payload, DateTime.UtcNow, 0.0, false);

            var resultOne = queue.Get(new QueryDocument(), TimeSpan.MaxValue);
            resultTwo = queue.Get(new QueryDocument(), TimeSpan.MaxValue);
            var resultThree = queue.Get(new QueryDocument(), TimeSpan.MaxValue);

            Assert.AreEqual(messageOne, resultOne.Payload);
            Assert.AreEqual(messageTwo, resultTwo.Payload);
            Assert.AreEqual(messageThree, resultThree.Payload);
        }

        [Test]
        public void GetWait()
        {
            var start = DateTime.Now;

            queue.Get(new QueryDocument(), TimeSpan.MaxValue, TimeSpan.FromMilliseconds(200), TimeSpan.FromMilliseconds(201), false);

            var end = DateTime.Now;

            Assert.IsTrue(end - start >= TimeSpan.FromMilliseconds(200));
            Assert.IsTrue(end - start < TimeSpan.FromMilliseconds(400));

            start = DateTime.Now;

            queue.Get(new QueryDocument(), TimeSpan.MaxValue, TimeSpan.FromMilliseconds(200), TimeSpan.MinValue, false);

            end = DateTime.Now;

            Assert.IsTrue(end - start >= TimeSpan.FromMilliseconds(200));
            Assert.IsTrue(end - start < TimeSpan.FromMilliseconds(400));
        }

        [Test]
        public void GetApproximateWait()
        {
            var min = double.MaxValue;
            var max = double.MinValue;
            for (var i = 0; i < 10; ++i)
            {
                var start = DateTime.Now;

                queue.Get(new QueryDocument(), TimeSpan.MaxValue, TimeSpan.FromMilliseconds(100), TimeSpan.MinValue, true);

                var time = (DateTime.Now - start).TotalMilliseconds;
                Assert.IsTrue(time >= 80.0);//minux 0.1 of 100
                Assert.IsTrue(time < 200.0);

                min = Math.Min(min, time);
                max = Math.Max(max, time);
            }

            Assert.IsTrue(min < 100.0);
            Assert.IsTrue(max > 100.0);
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

            using (var streamOne = new MemoryStream())
            using (var streamTwo = new MemoryStream())
            {
                streamOne.WriteByte(111);
                streamTwo.WriteByte(222);
                streamOne.Position = 0;
                streamTwo.Position = 0;
                queue.Send(messageOne, DateTime.Now, 0.0, new Dictionary<string, Stream>{ { "one", streamOne }, { "two", streamTwo }});
            }
            queue.Send(new BsonDocument("key", "value"));

            var result = queue.Get(new QueryDocument(messageOne), TimeSpan.MaxValue, TimeSpan.Zero);
            Assert.AreEqual(2, collection.Count());

            queue.Ack(result.Handle);
            Assert.AreEqual(1, collection.Count());
            Assert.AreEqual(0, gridfs.Files.Count());
            Assert.AreEqual(0, gridfs.Chunks.Count());
        }

        [Test]
        [ExpectedException(typeof(ArgumentNullException))]
        public void AckWithNullHandle()
        {
            queue.Ack(null);
        }
        #endregion

        #region AckMulti
        [Test]
        public void AckMultiMoreThanBatch()
        {
            using (var streamOne = new MemoryStream())
            using (var streamTwo = new MemoryStream())
            {
                streamOne.WriteByte(0);
                streamTwo.WriteByte(1);

                for (var i = 0; i < Queue.ACK_MULTI_BATCH_SIZE; ++i)
                {
                    var message = new BsonDocument("key", i);

                    streamOne.Position = 0;
                    streamTwo.Position = 0;
                    queue.Send(message, DateTime.Now, 0.0, new Dictionary<string, Stream>{ { "one", streamOne }, { "two", streamTwo }});
                }
            }

            queue.Send(new BsonDocument("key", "value"));

            var handles = new Handle[Queue.ACK_MULTI_BATCH_SIZE];
            for (var i = 0; i < handles.Length; ++i)
                handles[i] = queue.Get(new QueryDocument("key", i), TimeSpan.MaxValue, TimeSpan.Zero).Handle;

            Assert.AreEqual(Queue.ACK_MULTI_BATCH_SIZE + 1, collection.Count());
            Assert.AreEqual(Queue.ACK_MULTI_BATCH_SIZE * 2, gridfs.Files.Count());
            Assert.AreEqual(Queue.ACK_MULTI_BATCH_SIZE * 2, gridfs.Chunks.Count());

            queue.AckMulti(handles);

            Assert.AreEqual(1, collection.Count());
            Assert.AreEqual(0, gridfs.Files.Count());
            Assert.AreEqual(0, gridfs.Chunks.Count());
        }

        [Test]
        public void AckMultiLessThanBatch()
        {
            using (var streamOne = new MemoryStream())
            using (var streamTwo = new MemoryStream())
            {
                streamOne.WriteByte(0);
                streamTwo.WriteByte(1);
                streamOne.Position = 0;
                streamTwo.Position = 0;
                queue.Send(new BsonDocument("key", 0), DateTime.Now, 0.0, new Dictionary<string, Stream>{ { "one", streamOne }, { "two", streamTwo }});
            }

            queue.Send(new BsonDocument("key", 1));
            queue.Send(new BsonDocument("key", 2));

            var handles = new []
            {
                queue.Get(new QueryDocument("key", 0), TimeSpan.MaxValue, TimeSpan.Zero).Handle,
                queue.Get(new QueryDocument("key", 1), TimeSpan.MaxValue, TimeSpan.Zero).Handle,
            };

            Assert.AreEqual(3, collection.Count());
            Assert.AreEqual(2, gridfs.Files.Count());
            Assert.AreEqual(2, gridfs.Chunks.Count());

            queue.AckMulti(handles);

            Assert.AreEqual(1, collection.Count());
            Assert.AreEqual(0, gridfs.Files.Count());
            Assert.AreEqual(0, gridfs.Chunks.Count());
        }

        [Test]
        [ExpectedException(typeof(ArgumentNullException))]
        public void AckMultiWithNullHandles()
        {
            queue.AckMulti(null);
        }
        #endregion

        #region AckSend
        [Test]
        public void AckSend()
        {
            var messageOne = new BsonDocument { { "key1", 0 }, { "key2", true } };
            var messageThree = new BsonDocument { { "hi", "there" }, { "rawr", 2 } };

            using (var streamOne = new MemoryStream())
            using (var streamTwo = new MemoryStream())
            {
                streamOne.WriteByte(11);
                streamTwo.WriteByte(22);
                streamOne.Position = 0;
                streamTwo.Position = 0;
                queue.Send(messageOne, DateTime.Now, 0.0, new Dictionary<string, Stream> { { "one", streamOne }, { "two", streamTwo } });
            }
            queue.Send(new BsonDocument("key", "value"));

            var resultOne = queue.Get(new QueryDocument(messageOne), TimeSpan.MaxValue, TimeSpan.Zero);
            Assert.AreEqual(2, collection.Count());

            using (var streamOne = new MemoryStream())
            using (var streamTwo = new MemoryStream())
            {
                streamOne.WriteByte(111);
                streamTwo.WriteByte(222);
                streamOne.Position = 0;
                streamTwo.Position = 0;
                queue.AckSend(resultOne.Handle, messageThree, DateTime.Now, 0.0, true, new Dictionary<string, Stream> { { "one", streamOne }, { "two", streamTwo } });
            }
            Assert.AreEqual(2, collection.Count());

            var actual = queue.Get(new QueryDocument("hi", "there"), TimeSpan.MaxValue, TimeSpan.Zero);
            Assert.AreEqual(messageThree, actual.Payload);

            Assert.AreEqual(111, actual.Streams["one"].ReadByte());
            Assert.AreEqual(222, actual.Streams["two"].ReadByte());

            Assert.AreEqual(2, gridfs.Files.Count());
            Assert.AreEqual(2, gridfs.Chunks.Count());
        }

        [Test]
        public void AckSendOverloads()
        {
            var messageOne = new BsonDocument { { "key1", 0 }, { "key2", true } };
            var messageThree = new BsonDocument { { "hi", "there" }, { "rawr", 2 } };

            queue.Send(messageOne);
            queue.Send(new BsonDocument("key", "value"));

            var resultOne = queue.Get(new QueryDocument(messageOne), TimeSpan.MaxValue, TimeSpan.Zero);
            Assert.AreEqual(2, collection.Count());

            queue.AckSend(resultOne.Handle, messageThree);

            var resultTwo = queue.Get(new QueryDocument(messageThree), TimeSpan.MaxValue, TimeSpan.Zero);
            Assert.AreEqual(messageThree, resultTwo.Payload);
            Assert.AreEqual(2, collection.Count());

            queue.AckSend(resultTwo.Handle, messageOne, DateTime.Now);

            var resultThree = queue.Get(new QueryDocument(messageOne), TimeSpan.MaxValue, TimeSpan.Zero);
            Assert.AreEqual(messageOne, resultThree.Payload);
            Assert.AreEqual(2, collection.Count());

            queue.AckSend(resultThree.Handle, messageThree, DateTime.Now, 0.0);

            var resultFour = queue.Get(new QueryDocument(messageThree), TimeSpan.MaxValue, TimeSpan.Zero);
            Assert.AreEqual(messageThree, resultFour.Payload);
            Assert.AreEqual(2, collection.Count());

            queue.AckSend(resultFour.Handle, messageOne, DateTime.Now, 0.0, true);

            var resultFive = queue.Get(new QueryDocument(messageOne), TimeSpan.MaxValue, TimeSpan.Zero);
            Assert.AreEqual(messageOne, resultFive.Payload);
            Assert.AreEqual(2, collection.Count());
        }

        [Test]
        [ExpectedException(typeof(ArgumentException))]
        public void AckSendWitNanPriority()
        {
            queue.Send(new BsonDocument());
            var result = queue.Get(new QueryDocument(), TimeSpan.MaxValue);
            queue.AckSend(result.Handle, new BsonDocument("key", "value"), DateTime.Now, Double.NaN);
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
            queue.Send(new BsonDocument());
            var result = queue.Get(new QueryDocument(), TimeSpan.MaxValue);
            queue.AckSend(result.Handle, null);
        }

        [Test]
        public void AckSendWithNullStreams()
        {
            using (var streamOne = new MemoryStream())
            using (var streamTwo = new MemoryStream())
            {
                streamOne.WriteByte(11);
                streamTwo.WriteByte(22);
                streamOne.Position = 0;
                streamTwo.Position = 0;
                queue.Send(new BsonDocument(), DateTime.Now, 0.0, new Dictionary<string, Stream> { { "one", streamOne }, { "two", streamTwo } });
            }
            var resultOne = queue.Get(new QueryDocument(), TimeSpan.MaxValue);

            var messageTwo = new BsonDocument("key", "value");
            queue.AckSend(resultOne.Handle, messageTwo, DateTime.Now, 0.0, true, null);

            var resultTwo = queue.Get(new QueryDocument(messageTwo), TimeSpan.MaxValue);
            Assert.AreEqual(1, collection.Count());
            Assert.AreEqual(messageTwo, resultTwo.Payload);

            Assert.AreEqual(11, resultTwo.Streams["one"].ReadByte());
            Assert.AreEqual(22, resultTwo.Streams["two"].ReadByte());

            Assert.AreEqual(2, gridfs.Files.Count());
            Assert.AreEqual(2, gridfs.Chunks.Count());
        }
        #endregion

        #region Send
        [Test]
        public void Send()
        {
            var now = DateTime.Now;

            var payload = new BsonDocument { { "key1", 0 }, { "key2", true } };

            using (var streamOne = new MemoryStream())
            using (var streamTwo = new MemoryStream())
            {
                gridfs.Upload(streamOne, "one");//making sure same file names are ok as long as their ids are diffrent

                streamOne.WriteByte(111);
                streamTwo.WriteByte(222);
                streamOne.Position = 0;
                streamTwo.Position = 0;
                queue.Send(payload, now, 0.8, new Dictionary<string, Stream>{ { "one", streamOne }, { "two", streamTwo } });
            }

            var expected = new BsonDocument
            {
                //_id added below
                { "payload", payload },
                { "running", false },
                { "resetTimestamp", new BsonDateTime(DateTime.MaxValue) },
                { "earliestGet", new BsonDateTime(now) },
                { "priority", 0.8 },
                //streams added below
                //created added below
            };

            var message = collection.FindOneAs<BsonDocument>();

            var actualCreated = message["created"];
            expected["created"] = actualCreated;
            actualCreated = actualCreated.ToUniversalTime();

            var actualStreamIds = message["streams"].AsBsonArray;
            expected["streams"] = actualStreamIds;

            Assert.IsTrue(actualCreated <= DateTime.UtcNow);
            Assert.IsTrue(actualCreated > DateTime.UtcNow - TimeSpan.FromSeconds(10));

            expected.InsertAt(0, new BsonElement("_id", message["_id"]));
            Assert.AreEqual(expected, message);

            var fileOne = gridfs.FindOneById(actualStreamIds[0]);
            Assert.AreEqual("one", fileOne.Name);
            using (var stream = fileOne.OpenRead())
                Assert.AreEqual(111, stream.ReadByte());

            var fileTwo = gridfs.FindOneById(actualStreamIds[1]);
            Assert.AreEqual("two", fileTwo.Name);
            using (var stream = fileTwo.OpenRead())
                Assert.AreEqual(222, stream.ReadByte());
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

        [Test]
        [ExpectedException(typeof(ArgumentNullException))]
        public void SendWithNullStreams()
        {
            queue.Send(new BsonDocument("key", "value"), DateTime.Now, 0.0, null);
        }
        #endregion

        #region GetRandomDouble
        [Test]
        public void GetRandomDoubleFromZeroToOne()
        {
            var count = 1000;
            var sum = 0.0;
            for (var i = 0; i < count; ++i)
            {
                var randomDouble = Queue.GetRandomDouble(0.0, 1.0);
                sum += randomDouble;
                Assert.IsTrue(randomDouble <= 1.0);
                Assert.IsTrue(randomDouble >= 0.0);
            }

            var average = sum / (double)count;

            Assert.IsTrue(average >= 0.45);
            Assert.IsTrue(average <= 0.55);
        }

        [Test]
        public void GetRandomDoubleFromNegativeOneToPositiveOne()
        {
            var count = 1000;
            var sum = 0.0;
            for (var i = 0; i < count; ++i)
            {
                var randomDouble = Queue.GetRandomDouble(-1.0, 1.0);
                sum += randomDouble;
                Assert.IsTrue(randomDouble <= 1.0);
                Assert.IsTrue(randomDouble >= -1.0);
            }

            var average = sum / (double)count;

            Assert.IsTrue(average >= -0.05);
            Assert.IsTrue(average <= 0.05);
        }

        [Test]
        public void GetRandomDoubleFromThreeToFour()
        {
            var count = 1000;
            var sum = 0.0;
            for (var i = 0; i < count; ++i)
            {
                var randomDouble = Queue.GetRandomDouble(3.0, 4.0);
                sum += randomDouble;
                Assert.IsTrue(randomDouble <= 4.0);
                Assert.IsTrue(randomDouble >= 3.0);
            }

            var average = sum / (double)count;

            Assert.IsTrue(average >= 3.45);
            Assert.IsTrue(average <= 3.55);
        }

        [Test]
        [ExpectedException(typeof(ArgumentException))]
        public void GetRandomDoubleWithNaNMin()
        {
            Queue.GetRandomDouble(double.NaN, 4.0);
        }

        [Test]
        [ExpectedException(typeof(ArgumentException))]
        public void GetRandomDoubleWithNaNMax()
        {
            Queue.GetRandomDouble(4.0, double.NaN);
        }

        [Test]
        [ExpectedException(typeof(ArgumentException))]
        public void GetRandomDoubleWithMaxLessThanMin()
        {
            Queue.GetRandomDouble(4.0, 3.9);
        }
        #endregion
    }
}
