using System;
using System.Configuration;
using System.Threading;
using System.Linq;
using MongoDB.Bson;
using MongoDB.Driver;
using System.Reflection;
using System.Security.Cryptography;
using System.Collections.Generic;
using MongoDB.Driver.GridFS;
using System.IO;
using System.Runtime.CompilerServices;

[assembly: AssemblyVersion("2.0.0.*"), InternalsVisibleTo("DominionEnterprises.Mongo.Tests")]

namespace DominionEnterprises.Mongo
{
    /// <summary>
    /// Abstraction of mongo db collection as priority queue.
    /// </summary>
    /// <remarks>
    /// Tied priorities are then ordered by time. So you may use a single priority for normal queuing (overloads exist for this purpose).
    /// Using a random priority achieves random Get()
    /// </remarks>
    public sealed class Queue
    {
        internal const int ACK_MULTI_BATCH_SIZE = 1000;

        private readonly MongoCollection collection;
        private readonly MongoGridFS gridfs;

        /// <summary>
        /// Construct MongoQueue with url, db name and collection name from app settings keys mongoQueueUrl, mongoQueueDb and mongoQueueCollection
        /// </summary>
        public Queue()
            : this(ConfigurationManager.AppSettings["mongoQueueUrl"], ConfigurationManager.AppSettings["mongoQueueDb"], ConfigurationManager.AppSettings["mongoQueueCollection"])
        { }

        /// <summary>
        /// Construct MongoQueue
        /// </summary>
        /// <param name="url">mongo url like mongodb://localhost</param>
        /// <param name="db">db name</param>
        /// <param name="collection">collection name</param>
        /// <exception cref="ArgumentNullException">url, db or collection is null</exception>
        public Queue(string url, string db, string collection)
        {
            if (url == null) throw new ArgumentNullException("url");
            if (db == null) throw new ArgumentNullException("db");
            if (collection == null) throw new ArgumentNullException("collection");

            this.collection = new MongoClient(url).GetServer().GetDatabase(db).GetCollection(collection);
            this.gridfs = this.collection.Database.GetGridFS(MongoGridFSSettings.Defaults);
        }

        /// <summary>
        /// Construct MongoQueue
        /// </summary>
        /// <param name="collection">collection</param>
        /// <exception cref="ArgumentNullException">collection is null</exception>
        public Queue(MongoCollection collection)
        {
            if (collection == null) throw new ArgumentNullException("collection");

            this.collection = collection;
            this.gridfs = collection.Database.GetGridFS(MongoGridFSSettings.Defaults);
        }

        #region EnsureGetIndex
        /// <summary>
        /// Ensure index for Get() method with no fields before or after sort fields
        /// </summary>
        public void EnsureGetIndex()
        {
            EnsureGetIndex(new IndexKeysDocument());
        }

        /// <summary>
        /// Ensure index for Get() method with no fields after sort fields
        /// </summary>
        /// <param name="beforeSort">fields in Get() call that should be before the sort fields in the index</param>
        /// <exception cref="ArgumentNullException">beforeSort is null</exception>
        /// <exception cref="ArgumentException">beforeSort field value is not 1 or -1</exception>
        public void EnsureGetIndex(IndexKeysDocument beforeSort)
        {
            EnsureGetIndex(beforeSort, new IndexKeysDocument());
        }

        /// <summary>
        /// Ensure index for Get() method
        /// </summary>
        /// <param name="beforeSort">fields in Get() call that should be before the sort fields in the index</param>
        /// <param name="afterSort">fields in Get() call that should be after the sort fields in the index</param>
        /// <exception cref="ArgumentNullException">beforeSort or afterSort is null</exception>
        /// <exception cref="ArgumentException">beforeSort or afterSort field value is not 1 or -1</exception>
        public void EnsureGetIndex(IndexKeysDocument beforeSort, IndexKeysDocument afterSort)
        {
            if (beforeSort == null) throw new ArgumentNullException("beforeSort");
            if (afterSort == null) throw new ArgumentNullException("afterSort");

            //using general rule: equality, sort, range or more equality tests in that order for index
            var completeIndex = new IndexKeysDocument("running", 1);

            foreach (var field in beforeSort)
            {
                if (field.Value != 1 && field.Value != -1) throw new ArgumentException("field values must be 1 or -1 for ascending or descending", "beforeSort");
                completeIndex.Add("payload." + field.Name, field.Value);
            }

            completeIndex.Add("priority", 1);
            completeIndex.Add("created", 1);

            foreach (var field in afterSort)
            {
                if (field.Value != -1 && field.Value != 1) throw new ArgumentException("field values must be 1 or -1 for ascending or descending", "afterSort");
                completeIndex.Add("payload." + field.Name, field.Value);
            }

            completeIndex.Add("earliestGet", 1);

            EnsureIndex(completeIndex);//main query in Get()
            EnsureIndex(new IndexKeysDocument { { "running", 1 }, { "resetTimestamp", 1 } });//for the stuck messages query in Get()
        }
        #endregion

        /// <summary>
        /// Ensure index for Count() method
        /// Is a no-op if the generated index is a prefix of an existing one. If you have a similar EnsureGetIndex call, call it first.
        /// </summary>
        /// <param name="index">fields in Count() call</param>
        /// <param name="includeRunning">whether running was given to Count() or not</param>
        /// <exception cref="ArgumentNullException">index was null</exception>
        /// <exception cref="ArgumentException">index field value is not 1 or -1</exception>
        public void EnsureCountIndex(IndexKeysDocument index, bool includeRunning)
        {
            if (index == null) throw new ArgumentNullException("index");

            var completeFields = new IndexKeysDocument();

            if (includeRunning)
                completeFields.Add("running", 1);

            foreach (var field in index)
            {
                if (field.Value != 1 && field.Value != -1) throw new ArgumentException("field values must be 1 or -1 for ascending or descending", "index");
                completeFields.Add("payload." + field.Name, field.Value);
            }

            EnsureIndex(completeFields);
        }

        #region Get
        /// <summary>
        /// Get a non running message from queue with a wait of 3 seconds and poll of 200 milliseconds
        /// </summary>
        /// <param name="query">query where top level fields do not contain operators. Lower level fields can however. eg: valid {a: {$gt: 1}, "b.c": 3}, invalid {$and: [{...}, {...}]}</param>
        /// <param name="resetRunning">duration before this message is considered abandoned and will be given with another call to Get()</param>
        /// <returns>message or null</returns>
        /// <exception cref="ArgumentNullException">query is null</exception>
        public Message Get(QueryDocument query, TimeSpan resetRunning)
        {
            return Get(query, resetRunning, TimeSpan.FromSeconds(3));
        }

        /// <summary>
        /// Get a non running message from queue with a poll of 200 milliseconds
        /// </summary>
        /// <param name="query">query where top level fields do not contain operators. Lower level fields can however. eg: valid {a: {$gt: 1}, "b.c": 3}, invalid {$and: [{...}, {...}]}</param>
        /// <param name="resetRunning">duration before this message is considered abandoned and will be given with another call to Get()</param>
        /// <param name="wait">duration to keep polling before returning null</param>
        /// <returns>message or null</returns>
        /// <exception cref="ArgumentNullException">query is null</exception>
        public Message Get(QueryDocument query, TimeSpan resetRunning, TimeSpan wait)
        {
            return Get(query, resetRunning, wait, TimeSpan.FromMilliseconds(200));
        }

        /// <summary>
        /// Get a non running message from queue with an approxiate wait.
        /// </summary>
        /// <param name="query">query where top level fields do not contain operators. Lower level fields can however. eg: valid {a: {$gt: 1}, "b.c": 3}, invalid {$and: [{...}, {...}]}</param>
        /// <param name="resetRunning">duration before this message is considered abandoned and will be given with another call to Get()</param>
        /// <param name="wait">duration to keep polling before returning null</param>
        /// <param name="poll">duration between poll attempts</param>
        /// <returns>message or null</returns>
        /// <exception cref="ArgumentNullException">query is null</exception>
        public Message Get(QueryDocument query, TimeSpan resetRunning, TimeSpan wait, TimeSpan poll)
        {
            return Get(query, resetRunning, wait, poll, true);
        }

        /// <summary>
        /// Get a non running message from queue
        /// </summary>
        /// <param name="query">query where top level fields do not contain operators. Lower level fields can however. eg: valid {a: {$gt: 1}, "b.c": 3}, invalid {$and: [{...}, {...}]}</param>
        /// <param name="resetRunning">duration before this message is considered abandoned and will be given with another call to Get()</param>
        /// <param name="wait">duration to keep polling before returning null</param>
        /// <param name="poll">duration between poll attempts</param>
        /// <param name="approximateWait">whether to fluctuate the wait time randomly by +-10 percent. This ensures Get() calls seperate in time when multiple Queues are used in loops started at the same time</param>
        /// <returns>message or null</returns>
        /// <exception cref="ArgumentNullException">query is null</exception>
        public Message Get(QueryDocument query, TimeSpan resetRunning, TimeSpan wait, TimeSpan poll, bool approximateWait)
        {
            if (query == null)
                throw new ArgumentNullException ("query");

            //reset stuck messages
            collection.Update(
                new QueryDocument { { "running", true }, { "resetTimestamp", new BsonDocument("$lte", DateTime.UtcNow) } },
                new UpdateDocument("$set", new BsonDocument("running", false)),
                UpdateFlags.Multi
            );

            var builtQuery = new QueryDocument("running", false);
            foreach (var field in query)
                builtQuery.Add("payload." + field.Name, field.Value);

            builtQuery.Add("earliestGet", new BsonDocument("$lte", DateTime.UtcNow));

            var resetTimestamp = DateTime.UtcNow;
            try
            {
                resetTimestamp += resetRunning;
            }
            catch (ArgumentOutOfRangeException)
            {
                resetTimestamp = resetRunning > TimeSpan.Zero ? DateTime.MaxValue : DateTime.MinValue;
            }

            var sort = new SortByDocument { { "priority", 1 }, { "created", 1 } };
            var update = new UpdateDocument("$set", new BsonDocument { { "running", true }, { "resetTimestamp", resetTimestamp } });
            var fields = new FieldsDocument { { "payload", 1 }, { "streams", 1 } };

            var end = DateTime.UtcNow;
            try
            {
                if (approximateWait)
                    //fluctuate randomly by 10 percent
                    wait += TimeSpan.FromMilliseconds(wait.TotalMilliseconds * GetRandomDouble(-0.1, 0.1));

                end += wait;
            }
            catch (Exception e)
            {
                if (!(e is OverflowException) && !(e is ArgumentOutOfRangeException))
                    throw e;//cant cover

                end = wait > TimeSpan.Zero ? DateTime.MaxValue : DateTime.MinValue;
            }

            while (true)
            {
                var findModifyArgs = new FindAndModifyArgs { Query = builtQuery, SortBy = sort, Update = update, Fields = fields, Upsert = false };

                var message = collection.FindAndModify(findModifyArgs).ModifiedDocument;
                if (message != null)
                {
                    var handleStreams = new List<KeyValuePair<BsonValue, Stream>>();
                    var messageStreams = new Dictionary<string, Stream>();
                    foreach (var streamId in message["streams"].AsBsonArray)
                    {
                        var fileInfo = gridfs.FindOneById(streamId);

                        var stream = fileInfo.OpenRead();

                        handleStreams.Add(new KeyValuePair<BsonValue, Stream>(streamId, stream));
                        messageStreams.Add(fileInfo.Name, stream);
                    }

                    var handle = new Handle(message["_id"].AsObjectId, handleStreams);
                    return new Message(handle, message["payload"].AsBsonDocument, messageStreams);
                }

                if (DateTime.UtcNow >= end)
                    return null;

                try
                {
                    Thread.Sleep(poll);
                }
                catch (ArgumentOutOfRangeException)
                {
                    if (poll < TimeSpan.Zero)
                        poll = TimeSpan.Zero;
                    else
                        poll = TimeSpan.FromMilliseconds(int.MaxValue);

                    Thread.Sleep(poll);
                }

                if (DateTime.UtcNow >= end)
                    return null;
            }
        }
        #endregion

        #region Count
        /// <summary>
        /// Count in queue, running true or false
        /// </summary>
        /// <param name="query">query where top level fields do not contain operators. Lower level fields can however. eg: valid {a: {$gt: 1}, "b.c": 3}, invalid {$and: [{...}, {...}]}</param>
        /// <returns>count</returns>
        /// <exception cref="ArgumentNullException">query is null</exception>
        public long Count(QueryDocument query)
        {
            if (query == null) throw new ArgumentNullException("query");

            var completeQuery = new QueryDocument();

            foreach (var field in query)
                completeQuery.Add("payload." + field.Name, field.Value);

            return collection.Count(completeQuery);
        }

        /// <summary>
        /// Count in queue
        /// </summary>
        /// <param name="query">query where top level fields do not contain operators. Lower level fields can however. eg: valid {a: {$gt: 1}, "b.c": 3}, invalid {$and: [{...}, {...}]}</param>
        /// <param name="running">count running messages or not running</param>
        /// <returns>count</returns>
        /// <exception cref="ArgumentNullException">query is null</exception>
        public long Count(QueryDocument query, bool running)
        {
            if (query == null) throw new ArgumentNullException("query");

            var completeQuery = new QueryDocument("running", running);
            foreach (var field in query)
                completeQuery.Add("payload." + field.Name, field.Value);

            return collection.Count(completeQuery);
        }
        #endregion

        /// <summary>
        /// Acknowledge a handle was processed and remove from queue.
        /// </summary>
        /// <param name="handle">handle received from Get()</param>
        /// <exception cref="ArgumentNullException">handle is null</exception>
        public void Ack(Handle handle)
        {
            if (handle == null) throw new ArgumentNullException("handle");

            collection.Remove(new QueryDocument("_id", handle.Id));

            foreach (var stream in handle.Streams)
            {
                stream.Value.Dispose();
                gridfs.DeleteById(stream.Key);
            }
        }

        /// <summary>
        /// Acknowledge multiple handles were processed and remove from queue.
        /// </summary>
        /// <param name="handles">handles received from Get()</param>
        /// <exception cref="ArgumentNullException">handles is null</exception>
        public void AckMulti(IEnumerable<Handle> handles)
        {
            if (handles == null) throw new ArgumentNullException("handles");

            var ids = new BsonArray();
            foreach (var handle in handles)
            {
                ids.Add(handle.Id);

                if (ids.Count != ACK_MULTI_BATCH_SIZE)
                    continue;

                collection.Remove(new QueryDocument("_id", new BsonDocument("$in", ids)));
                ids.Clear();
            }

            if (ids.Count > 0)
                collection.Remove(new QueryDocument("_id", new BsonDocument("$in", ids)));

            foreach (var handle in handles)
            {
                foreach (var stream in handle.Streams)
                {
                    stream.Value.Dispose();
                    gridfs.DeleteById(stream.Key);
                }
            }
        }

        #region AckSend
        /// <summary>
        /// Ack handle and send payload to queue, atomically, with earliestGet as Now, 0.0 priority, new timestamp and no gridfs streams
        /// </summary>
        /// <param name="handle">handle to ack received from Get()</param>
        /// <param name="payload">payload to send</param>
        /// <exception cref="ArgumentNullException">handle or payload is null</exception>
        public void AckSend(Handle handle, BsonDocument payload)
        {
            AckSend(handle, payload, DateTime.UtcNow);
        }

        /// <summary>
        /// Ack handle and send payload to queue, atomically, with 0.0 priority, new timestamp and no gridfs streams
        /// </summary>
        /// <param name="handle">handle to ack received from Get()</param>
        /// <param name="payload">payload to send</param>
        /// <param name="earliestGet">earliest instant that a call to Get() can return message</param>
        /// <exception cref="ArgumentNullException">handle or payload is null</exception>
        public void AckSend(Handle handle, BsonDocument payload, DateTime earliestGet)
        {
            AckSend(handle, payload, earliestGet, 0.0);
        }

        /// <summary>
        /// Ack handle and send payload to queue, atomically, with new timestamp and no gridfs streams
        /// </summary>
        /// <param name="handle">handle to ack received from Get()</param>
        /// <param name="payload">payload to send</param>
        /// <param name="earliestGet">earliest instant that a call to Get() can return message</param>
        /// <param name="priority">priority for order out of Get(). 0 is higher priority than 1</param>
        /// <exception cref="ArgumentNullException">handle or payload is null</exception>
        /// <exception cref="ArgumentException">priority was NaN</exception>
        public void AckSend(Handle handle, BsonDocument payload, DateTime earliestGet, double priority)
        {
            AckSend(handle, payload, earliestGet, priority, true);
        }

        /// <summary>
        /// Ack handle and send payload to queue, atomically, with no gridfs streams
        /// </summary>
        /// <param name="handle">handle to ack received from Get()</param>
        /// <param name="payload">payload to send</param>
        /// <param name="earliestGet">earliest instant that a call to Get() can return message</param>
        /// <param name="priority">priority for order out of Get(). 0 is higher priority than 1</param>
        /// <param name="newTimestamp">true to give the payload a new timestamp or false to use given message timestamp</param>
        /// <exception cref="ArgumentNullException">handle or payload is null</exception>
        /// <exception cref="ArgumentException">priority was NaN</exception>
        public void AckSend(Handle handle, BsonDocument payload, DateTime earliestGet, double priority, bool newTimestamp)
        {
            AckSend(handle, payload, earliestGet, priority, newTimestamp, new KeyValuePair<string, Stream>[0]);
        }

        /// <summary>
        /// Ack handle and send payload to queue, atomically.
        /// </summary>
        /// <param name="handle">handle to ack received from Get()</param>
        /// <param name="payload">payload to send</param>
        /// <param name="earliestGet">earliest instant that a call to Get() can return message</param>
        /// <param name="priority">priority for order out of Get(). 0 is higher priority than 1</param>
        /// <param name="newTimestamp">true to give the payload a new timestamp or false to use given message timestamp</param>
        /// <param name="streams">streams to upload into gridfs or null to forward handle's streams</param>
        /// <exception cref="ArgumentNullException">handle or payload is null</exception>
        /// <exception cref="ArgumentException">priority was NaN</exception>
        public void AckSend(Handle handle, BsonDocument payload, DateTime earliestGet, double priority, bool newTimestamp, IEnumerable<KeyValuePair<string, Stream>> streams)
        {
            if (handle == null) throw new ArgumentNullException("handle");
            if (payload == null) throw new ArgumentNullException("payload");
            if (Double.IsNaN(priority)) throw new ArgumentException("priority was NaN", "priority");

            var toSet = new BsonDocument
            {
                {"payload", payload},
                {"running", false},
                {"resetTimestamp", DateTime.MaxValue},
                {"earliestGet", earliestGet},
                {"priority", priority},
            };
            if (newTimestamp)
                toSet["created"] = DateTime.UtcNow;

            if (streams != null)
            {
                var streamIds = new BsonArray();
                foreach (var stream in streams)
                    streamIds.Add(gridfs.Upload(stream.Value, stream.Key).Id);

                toSet["streams"] = streamIds;
            }

            //using upsert because if no documents found then the doc was removed (SHOULD ONLY HAPPEN BY SOMEONE MANUALLY) so we can just send
            collection.Update(new QueryDocument("_id", handle.Id), new UpdateDocument("$set", toSet), UpdateFlags.Upsert);

            foreach (var existingStream in handle.Streams)
                existingStream.Value.Dispose();

            if (streams != null)
            {
                foreach (var existingStream in handle.Streams)
                    gridfs.DeleteById(existingStream.Key);
            }
        }
        #endregion

        #region Send
        /// <summary>
        /// Send message to queue with earliestGet as Now, 0.0 priority and no gridfs streams
        /// </summary>
        /// <param name="payload">payload</param>
        /// <exception cref="ArgumentNullException">payload is null</exception>
        public void Send(BsonDocument payload)
        {
            Send(payload, DateTime.UtcNow, 0.0);
        }

        /// <summary>
        /// Send message to queue with 0.0 priority and no gridfs streams
        /// </summary>
        /// <param name="payload">payload</param>
        /// <param name="earliestGet">earliest instant that a call to Get() can return message</param>
        /// <exception cref="ArgumentNullException">payload is null</exception>
        public void Send(BsonDocument payload, DateTime earliestGet)
        {
            Send(payload, earliestGet, 0.0);
        }

        /// <summary>
        /// Send message to queue with no gridfs streams
        /// </summary>
        /// <param name="payload">payload</param>
        /// <param name="earliestGet">earliest instant that a call to Get() can return message</param>
        /// <param name="priority">priority for order out of Get(). 0 is higher priority than 1</param>
        /// <exception cref="ArgumentNullException">payload is null</exception>
        /// <exception cref="ArgumentException">priority was NaN</exception>
        public void Send(BsonDocument payload, DateTime earliestGet, double priority)
        {
            Send(payload, earliestGet, priority, new List<KeyValuePair<string, Stream>>());
        }

        /// <summary>
        /// Send message to queue.
        /// </summary>
        /// <param name="payload">payload</param>
        /// <param name="earliestGet">earliest instant that a call to Get() can return message</param>
        /// <param name="priority">priority for order out of Get(). 0 is higher priority than 1</param>
        /// <param name="streams">streams to upload into gridfs</param>
        /// <exception cref="ArgumentNullException">payload is null</exception>
        /// <exception cref="ArgumentException">priority was NaN</exception>
        /// <exception cref="ArgumentNullException">streams is null</exception>
        public void Send(BsonDocument payload, DateTime earliestGet, double priority, IEnumerable<KeyValuePair<string, Stream>> streams)
        {
            if (payload == null) throw new ArgumentNullException("payload");
            if (Double.IsNaN(priority)) throw new ArgumentException("priority was NaN", "priority");
            if (streams == null) throw new ArgumentNullException("streams");

            var streamIds = new BsonArray();
            foreach (var stream in streams)
                streamIds.Add(gridfs.Upload(stream.Value, stream.Key).Id);

            var message = new BsonDocument
            {
                {"payload", payload},
                {"running", false},
                {"resetTimestamp", DateTime.MaxValue},
                {"earliestGet", earliestGet},
                {"priority", priority},
                {"created", DateTime.UtcNow},
                {"streams", streamIds},
            };

            collection.Insert(message);
        }
        #endregion

        private void EnsureIndex(IndexKeysDocument index)
        {
            //if index is a prefix of any existing index we are good
            foreach (var existingIndex in collection.GetIndexes())
            {
                var names = index.Names;
                var values = index.Values;
                var existingNamesPrefix = existingIndex.Key.Names.Take(names.Count());
                var existingValuesPrefix = existingIndex.Key.Values.Take(values.Count());

                if (Enumerable.SequenceEqual(names, existingNamesPrefix) && Enumerable.SequenceEqual(values, existingValuesPrefix))
                    return;
            }

            for (var i = 0; i < 5; ++i)
            {
                for (var name = Guid.NewGuid().ToString(); name.Length > 0; name = name.Substring(0, name.Length - 1))
                {
                    //creating an index with the same name and different spec does nothing.
                    //creating an index with same spec and different name does nothing.
                    //so we use any generated name, and then find the right spec after we have called, and just go with that name.

                    try
                    {
                        collection.CreateIndex(index, new IndexOptionsDocument { {"name", name }, { "background", true } });
                    }
                    catch (MongoCommandException)
                    {
                        //this happens when the name was too long
                    }

                    foreach (var existingIndex in collection.GetIndexes())
                    {
                        if (existingIndex.Key == index)
                            return;
                    }
                }
            }

            throw new Exception("couldnt create index after 5 attempts");
        }

        /// <summary>
        /// Gets a random double between min and max using RNGCryptoServiceProvider
        /// </summary>
        /// <returns>
        /// random double.
        /// </returns>
        internal static double GetRandomDouble(double min, double max)
        {
            if (Double.IsNaN(min)) throw new ArgumentException("min cannot be NaN");
            if (Double.IsNaN(max)) throw new ArgumentException("max cannot be NaN");
            if (max < min) throw new ArgumentException("max cannot be less than min");

            var buffer = new byte[8];
            new RNGCryptoServiceProvider().GetBytes(buffer);
            var randomULong = BitConverter.ToUInt64(buffer, 0);

            var fraction = (double)randomULong / (double)ulong.MaxValue;
            var fractionOfNewRange = fraction * (max - min);
            return min + fractionOfNewRange;
        }
    }

    /// <summary>
    /// Message to be given out of Get()
    /// </summary>
    public sealed class Message
    {
        public readonly Handle Handle;
        public readonly BsonDocument Payload;
        public readonly IDictionary<string, Stream> Streams;

        /// <summary>
        /// Construct Message
        /// </summary>
        /// <param name="handle">handle</param>
        /// <param name="payload">payload</param>
        /// <param name="streams">streams</param>
        internal Message(Handle handle, BsonDocument payload, IDictionary<string, Stream> streams)
        {
            this.Handle = handle;
            this.Payload = payload;
            this.Streams = streams;
        }
    }

    /// <summary>
    /// Message handle to be given to Ack() and AckSend().
    /// </summary>
    public sealed class Handle
    {
        internal readonly BsonObjectId Id;
        internal readonly IEnumerable<KeyValuePair<BsonValue, Stream>> Streams;

        /// <summary>
        /// Construct Handle
        /// </summary>
        /// <param name="id">id</param>
        /// <param name="streams">streams</param>
        internal Handle(BsonObjectId id, IEnumerable<KeyValuePair<BsonValue, Stream>> streams)
        {
            this.Id = id;
            this.Streams = streams;
        }
    }
}
