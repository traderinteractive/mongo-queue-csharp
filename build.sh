xbuild DominionEnterprises.Mongo.Tests/DominionEnterprises.Mongo.Tests.csproj \
&& mono tools.SharpCover/SharpCover.exe instrument travisCoverageConfig.json \
&& nunit-console DominionEnterprises.Mongo.Tests/bin/Debug/DominionEnterprises.Mongo.Tests.dll \
&& mono tools.SharpCover/SharpCover.exe check
