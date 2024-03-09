using Microsoft.DurableTask.Client;
using Microsoft.Extensions.Hosting;

var host = new HostBuilder()
    .ConfigureFunctionsWorkerDefaults()    
    .Build();

host.Run();
