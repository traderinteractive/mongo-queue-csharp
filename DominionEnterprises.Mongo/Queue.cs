using System;
using System.Configuration;
using System.Threading;
using MongoDB.Bson;
using MongoDB.Driver;

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
        private readonly MongoCollection collection;

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
        }

        #region EnsureGetIndex
        /// <summary>
        /// Ensure index for Get() method with no fields before or after sort fields
        /// </summary>
        public void EnsureGetIndex()
        {
            EnsureGetIndex(new IndexKeysDocument(), new IndexKeysDocument());
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
            completeIndex.Add("_id", 1);

            foreach (var field in afterSort)
            {
                if (field.Value != 1 && field.Value != -1) throw new ArgumentException("field values must be 1 or -1 for ascending or descending", "afterSort");
                completeIndex.Add("payload." + field.Name, field.Value);
            }

            completeIndex.Add("earliestGet", 1);

            EnsureIndex(completeIndex);//main query in Get()
            EnsureIndex(new IndexKeysDocument { { "running", 1 }, { "resetTimestamp", 1 } });//for the stuck messages query in Get()
        }
        #endregion

        /// <summary>
        /// Ensure index for Count() method
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
        public BsonDocument Get(QueryDocument query, TimeSpan resetRunning)
        {
            return Get(query, resetRunning, TimeSpan.FromSeconds(3), TimeSpan.FromMilliseconds(200));
        }

        /// <summary>
        /// Get a non running message from queue with a poll of 200 milliseconds
        /// </summary>
        /// <param name="query">query where top level fields do not contain operators. Lower level fields can however. eg: valid {a: {$gt: 1}, "b.c": 3}, invalid {$and: [{...}, {...}]}</param>
        /// <param name="resetRunning">duration before this message is considered abandoned and will be given with another call to Get()</param>
        /// <param name="wait">duration to keep polling before returning null</param>
        /// <returns>message or null</returns>
        /// <exception cref="ArgumentNullException">query is null</exception>
        public BsonDocument Get(QueryDocument query, TimeSpan resetRunning, TimeSpan wait)
        {
            return Get(query, resetRunning, wait, TimeSpan.FromMilliseconds(200));
        }

        /// <summary>
        /// Get a non running message from queue
        /// </summary>
        /// <param name="query">query where top level fields do not contain operators. Lower level fields can however. eg: valid {a: {$gt: 1}, "b.c": 3}, invalid {$and: [{...}, {...}]}</param>
        /// <param name="resetRunning">duration before this message is considered abandoned and will be given with another call to Get()</param>
        /// <param name="wait">duration to keep polling before returning null</param>
        /// <param name="poll">duration between poll attempts</param>
        /// <returns>message or null</returns>
        /// <exception cref="ArgumentNullException">query is null</exception>
        public BsonDocument Get(QueryDocument query, TimeSpan resetRunning, TimeSpan wait, TimeSpan poll)
        {
            if (query == null) throw new ArgumentNullException("query");

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

            var sort = new SortByDocument { { "priority", 1 }, { "_id", 1 } };
            var update = new UpdateDocument("$set", new BsonDocument { { "running", true }, { "resetTimestamp", resetTimestamp } });
            var fields = new FieldsDocument("payload", 1);

            var end = DateTime.UtcNow;
            try
            {
                end += wait;
            }
            catch (ArgumentOutOfRangeException)
            {
                end = wait > TimeSpan.Zero ? DateTime.MaxValue : DateTime.MinValue;
            }

            while (true)
            {
                var message = collection.FindAndModify(builtQuery, sort, update, fields, false, false).ModifiedDocument;
                if (message != null)
                    //using merge without overwriting so a possible id in payload doesnt wipe it out the generated one
                    return new BsonDocument("id", message["_id"]).Merge(message["payload"].AsBsonDocument);

                if (DateTime.UtcNow >= end)
                    return null;

                try
                {
                    Thread.Sleep(poll);
                }
                catch (ArgumentOutOfRangeException)
                {
                    poll = poll < TimeSpan.Zero ? TimeSpan.Zero : TimeSpan.FromMilliseconds(int.MaxValue);

                    Thread.Sleep(poll);
                }
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
        /// Acknowledge a message was processed and remove from queue.
        /// </summary>
        /// <param name="message">message received from Get()</param>
        /// <exception cref="ArgumentNullException">message is null</exception>
        /// <exception cref="ArgumentException">message id must be a BsonObjectId</exception>
        public void Ack(BsonDocument message)
        {
            if (message == null) throw new ArgumentNullException("message");
            var id = message["id"];
            if (id.GetType() != typeof(BsonObjectId)) throw new ArgumentException("id must be a BsonObjectId", "message");

            collection.Remove(new QueryDocument("_id", id));
        }

        #region AckSend
        /// <summary>
        /// Ack message and send payload to queue, atomically, with earliestGet as Now and 0.0 priority
        /// </summary>
        /// <param name="message">message to ack received from Get()</param>
        /// <param name="payload">payload to send</param>
        /// <exception cref="ArgumentNullException">message or payload is null</exception>
        /// <exception cref="ArgumentException">message id must be a BsonObjectId</exception>
        public void AckSend(BsonDocument message, BsonDocument payload)
        {
            AckSend(message, payload, DateTime.UtcNow, 0.0);
        }

        /// <summary>
        /// Ack message and send payload to queue, atomically, with 0.0 priority
        /// </summary>
        /// <param name="message">message to ack received from Get()</param>
        /// <param name="payload">payload to send</param>
        /// <param name="earliestGet">earliest instant that a call to Get() can return message</param>
        /// <exception cref="ArgumentNullException">message or payload is null</exception>
        /// <exception cref="ArgumentException">message id must be a BsonObjectId</exception>
        public void AckSend(BsonDocument message, BsonDocument payload, DateTime earliestGet)
        {
            AckSend(message, payload, earliestGet, 0.0);
        }

        /// <summary>
        /// Ack message and send payload to queue, atomically.
        /// </summary>
        /// <param name="message">message to ack received from Get()</param>
        /// <param name="payload">payload to send</param>
        /// <param name="earliestGet">earliest instant that a call to Get() can return message</param>
        /// <param name="priority">priority for order out of Get(). 0 is higher priority than 1</param>
        /// <exception cref="ArgumentNullException">message or payload is null</exception>
        /// <exception cref="ArgumentException">message id must be a BsonObjectId</exception>
        /// <exception cref="ArgumentException">priority was NaN</exception>
        public void AckSend(BsonDocument message, BsonDocument payload, DateTime earliestGet, double priority)
        {
            if (message == null) throw new ArgumentNullException("message");
            if (payload == null) throw new ArgumentNullException("payload");
            if (Double.IsNaN(priority)) throw new ArgumentException("priority was NaN", "priority");

            var messageId = message["id"];
            if (messageId.GetType() != typeof(BsonObjectId)) throw new ArgumentException("message id must be a BsonObjectId", "message");

            var newMessage = new UpdateDocument
            {
                {"payload", payload},
                {"running", false},
                {"resetTimestamp", DateTime.MaxValue},
                {"earliestGet", earliestGet},
                {"priority", priority},
            };

            //using upsert because if no documents found then the doc was removed (SHOULD ONLY HAPPEN BY SOMEONE MANUALLY) so we can just send
            collection.Update(new QueryDocument("_id", messageId), newMessage, UpdateFlags.Upsert);
        }
        #endregion

        #region Requeue
        /// <summary>
        /// Requeue message with earliestGet as Now and 0.0 priority. Same as AckSend() with the same message.
        /// </summary>
        /// <param name="message">message</param>
        /// <exception cref="ArgumentNullException">message is null</exception>
        /// <exception cref="ArgumentException">message id must be a BsonObjectId</exception>
        public void Requeue(BsonDocument message)
        {
            if (message == null) throw new ArgumentNullException("message");

            var forRequeue = new BsonDocument(message);
            forRequeue.Remove("id");
            AckSend(message, forRequeue, DateTime.UtcNow, 0.0);
        }

        /// <summary>
        /// Requeue message with 0.0 priority. Same as AckSend() with the same message.
        /// </summary>
        /// <param name="message">message</param>
        /// <param name="earliestGet">earliest instant that a call to Get() can return message</param>
        /// <exception cref="ArgumentNullException">message is null</exception>
        /// <exception cref="ArgumentException">message id must be a BsonObjectId</exception>
        public void Requeue(BsonDocument message, DateTime earliestGet)
        {
            if (message == null) throw new ArgumentNullException("message");

            var forRequeue = new BsonDocument(message);
            forRequeue.Remove("id");
            AckSend(message, forRequeue, earliestGet, 0.0);
        }

        /// <summary>
        /// Requeue message. Same as AckSend() with the same message.
        /// </summary>
        /// <param name="message">message</param>
        /// <param name="earliestGet">earliest instant that a call to Get() can return message</param>
        /// <param name="priority">priority for order out of Get(). 0 is higher priority than 1</param>
        /// <exception cref="ArgumentNullException">message is null</exception>
        /// <exception cref="ArgumentException">message id must be a BsonObjectId</exception>
        /// <exception cref="ArgumentException">priority was NaN</exception>
        public void Requeue(BsonDocument message, DateTime earliestGet, double priority)
        {
            if (message == null) throw new ArgumentNullException("message");

            var forRequeue = new BsonDocument(message);
            forRequeue.Remove("id");
            AckSend(message, forRequeue, earliestGet, priority);
        }
        #endregion

        #region Send
        /// <summary>
        /// Send message to queue with earliestGet as Now and 0.0 priority
        /// </summary>
        /// <param name="payload">payload</param>
        /// <exception cref="ArgumentNullException">payload is null</exception>
        public void Send(BsonDocument payload)
        {
            Send(payload, DateTime.UtcNow, 0.0);
        }

        /// <summary>
        /// Send message to queue with 0.0 priority
        /// </summary>
        /// <param name="payload">payload</param>
        /// <param name="earliestGet">earliest instant that a call to Get() can return message</param>
        /// <exception cref="ArgumentNullException">payload is null</exception>
        public void Send(BsonDocument payload, DateTime earliestGet)
        {
            Send(payload, earliestGet, 0.0);
        }

        /// <summary>
        /// Send message to queue.
        /// </summary>
        /// <param name="payload">payload</param>
        /// <param name="earliestGet">earliest instant that a call to Get() can return message</param>
        /// <param name="priority">priority for order out of Get(). 0 is higher priority than 1</param>
        /// <exception cref="ArgumentNullException">payload is null</exception>
        /// <exception cref="ArgumentException">priority was NaN</exception>
        public void Send(BsonDocument payload, DateTime earliestGet, double priority)
        {
            if (payload == null) throw new ArgumentNullException("payload");
            if (Double.IsNaN(priority)) throw new ArgumentException("priority was NaN", "priority");

            var message = new BsonDocument
            {
                {"payload", payload},
                {"running", false},
                {"resetTimestamp", DateTime.MaxValue},
                {"earliestGet", earliestGet},
                {"priority", priority},
            };

            collection.Insert(message);
        }
        #endregion

        private void EnsureIndex(IndexKeysDocument index)
        {
            for (var i = 0; i < 5; ++i)
            {
                for (var name = Guid.NewGuid().ToString(); name.Length > 0; name = name.Substring(0, name.Length - 1))
                {
                    //creating an index with the same name and different spec does nothing.
                    //creating an index with same spec and different name does nothing.
                    //so we use any generated name, and then find the right spec after we have called, and just go with that name.

                    try
                    {
                        collection.EnsureIndex(index, new IndexOptionsDocument("name", name));
                    }
                    catch (MongoCommandException)
                    {
                        //this happens when the name was too long
                        continue;
                    }

                    foreach (var existingIndex in collection.GetIndexes())
                    {
                        if (existingIndex.Key == index)
                            return;
                    }
                }
            }

            throw new Exception("couldnt create index after 100 attempts");
        }
    }
}
