#mongo-queue-csharp
[![Build Status](https://travis-ci.org/dominionenterprises/mongo-queue-csharp.png)](https://travis-ci.org/dominionenterprises/mongo-queue-csharp)

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

For linux make sure [Mono](https://github.com/mono/mono) which comes with [xbuild](http://www.mono-project.com/Microsoft.Build) is installed.
For windows make sure [.NET SDK](http://www.microsoft.com/en-us/download/details.aspx?id=8279) which comes with
[MSBuild](http://msdn.microsoft.com/en-us/library/dd393574.aspx) is installed.
Make sure you are in the repository root.

For linux run:

```bash
xbuild DominionEnterprises.Mongo/DominionEnterprises.Mongo.csproj
```

For windows run:

```dos
C:\Windows\Microsoft.NET\Framework64\v4.0.30319\MSBuild.exe DominionEnterprises.Mongo\DominionEnterprises.Mongo.csproj
```

and use the resulting `DominionEnterprises.Mongo/bin/Debug/DominionEnterprises.Mongo.dll` as a reference in your project.

##Documentation

Found in the [source](DominionEnterprises.Mongo/Queue.cs) itself, take a look!

##Contact

Developers may be contacted at:

 * [Pull Requests](https://github.com/dominionenterprises/mongo-queue-php/pulls)
 * [Issues](https://github.com/dominionenterprises/mongo-queue-php/issues)

##Project build

Install and start [mongodb](http://www.mongodb.org).
For linux make sure [Mono](https://github.com/mono/mono) which comes with [xbuild](http://www.mono-project.com/Microsoft.Build) is installed.
For windows make sure [.NET SDK](http://www.microsoft.com/en-us/download/details.aspx?id=8279) which comes with
[MSBuild](http://msdn.microsoft.com/en-us/library/dd393574.aspx) is installed.
For both make sure [nunit-console](http://www.nunit.org/index.php?p=nunit-console&r=2.2.10) is installed.
Make sure you are in the repository root.

For linux run:

```bash
sh build.sh
```

For windows run:

```dos
build.bat
```
