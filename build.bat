C:\Windows\Microsoft.NET\Framework64\v4.0.30319\MSBuild.exe DominionEnterprises.Mongo.Tests\DominionEnterprises.Mongo.Tests.csproj
if %errorlevel% neq 0 exit /B 1
tools.SharpCover\SharpCover.exe instrument travisCoverageConfig.json
if %errorlevel% neq 0 exit /B 1
"C:\Program Files (x86)\NUnit 2.6.2\bin\nunit-console.exe" DominionEnterprises.Mongo.Tests\bin\Debug\DominionEnterprises.Mongo.Tests.dll
if %errorlevel% neq 0 exit /B 1
tools.SharpCover\SharpCover.exe check
