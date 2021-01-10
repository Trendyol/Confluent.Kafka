using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;

namespace TestApplication
{
    public class MyHostedService : BackgroundService
    {
        private readonly MyConsumer _myConsumer;
        
        public MyHostedService(MyConsumer myConsumer)
        {
            _myConsumer = myConsumer;
        }
        
        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            await _myConsumer.RunAsync(stoppingToken);
        }
    }
}