#mongo-queue-csharp

C# message queue using MongoDB as a backend.
Adheres to the 1.0.0 [specification](https://github.com/dominionenterprises/mongo-queue-specification).

##Features

 * Message selection and/or count via MongoDB query
 * Distributes across machines via MongoDB
 * Multi language support through the [specification](https://github.com/dominionenterprises/mongo-queue-specification)
 * Message priority
 * Delayed messages
 * Running message timeout and redeliver
 * Atomic acknowledge and send together
 * Easy index creation based only on payload

##Simplest use

```csharp
using System;
using MongoDB.Bson;
using MongoDB.Driver;
using DominionEnterprises.Mongo;

var queue = new Queue("mongodb://localhost", "queues", "queue");
queue.Send(new BsonDocument());
var message = queue.Get(new QueryDocument(), TimeSpan.FromMinutes(1));
queue.Ack(message);
```

##Build

To add the library as a local, per-project dependency make sure [xbuild](http://www.mono-project.com/Microsoft.Build) are in your PATH and run:

```bash
xbuild DominionEnterprises.Mongo/DominionEnterprises.Mongo.csproj
```

and use the resulting `DominionEnterprises.Mongo/bin/Debug/DominionEnterprises.Mongo.dll` in your project.

##Documentation

Found in the [source](DominionEnterprises.Mongo/Queue.cs) itself, take a look!

##Contact

Developers may be contacted at:

 * [Email](mailto:tol_igroup@dominionenterprises.com)
 * [Pull Requests](https://github.com/dominionenterprises/mongo-queue-php/pulls)
 * [Issues](https://github.com/dominionenterprises/mongo-queue-php/issues)

##Tests

With a checkout of the code make sure [xbuild](http://www.mono-project.com/Microsoft.Build)
and [nunit-console](http://www.nunit.org/index.php?p=nunit-console&r=2.2.10) is in your PATH and run:

```bash
xbuild DominionEnterprises.Mongo.Tests/DominionEnterprises.Mongo.Tests.csproj
nunit-console DominionEnterprises.Mongo.Tests/bin/Debug/DominionEnterprises.Mongo.Tests.dll
```
