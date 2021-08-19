using System;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Synchrony.Core;

namespace Synchrony.Test
{
    class Program
    {
        static async Task Main(string[] args)
        {
            SynchronyDbContext dbContext = new SynchronyDbContext("", 1);
            await dbContext.Initialize(new CancellationToken());
            dbContext.Add += (sender, e) =>
            {
                Console.WriteLine(JsonSerializer.Serialize(e));
            };
            dbContext.Delete += (sender, e) =>
            {
                Console.WriteLine(JsonSerializer.Serialize(e));
            };
            dbContext.Update += (sender, e) =>
            {
                Console.WriteLine(JsonSerializer.Serialize(e));
            };
            Timer timer = new Timer(async (x) =>
            {
                await dbContext.GetChanges(new CancellationToken());
                Console.WriteLine(DateTime.Now);
            }, null, TimeSpan.Zero, TimeSpan.FromSeconds(1));
            Console.ReadLine();
        }
    }
}
