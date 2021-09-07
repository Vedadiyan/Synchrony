using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using NATS.Client;
using Synchrony.Core;

namespace Synchrony.Streaming
{
    class Program
    {
        static async Task Main(string[] args)
        {
            var _args = args.Select((Value, Index) => new { Value, Index });
            int? urlIndex = _args.Where(x => x.Value == "--NATS_URL").FirstOrDefault()?.Index;
            if (urlIndex == null)
            {
                throw new ArgumentException("NATS_URL is not set");
            }
            int? dbConnectionIndex = _args.Where(x => x.Value == "--CONNECTION_STRING").FirstOrDefault()?.Index;
            if (dbConnectionIndex == null)
            {
                throw new ArgumentException("CONNECTION_STRING is not set");
            }
            int? subjectNamespaceIndex = _args.Where(x => x.Value == "--SUBJECT_NAMESPACE").FirstOrDefault()?.Index;
            if (subjectNamespaceIndex == null)
            {
                throw new ArgumentException("SUBJECT_NAMESPACE is not set");
            }
            int? timeoutIndex = _args.Where(x => x.Value == "--TIMEOUT").FirstOrDefault()?.Index;
            NATSConnectorConfiguration natsConnectorConfiguration = new NATSConnectorConfiguration
            {
                NATSUrls = args[urlIndex.Value + 1],
                DbConnectionString = args[dbConnectionIndex.Value + 1],
                Timeout = timeoutIndex != null ? int.Parse(args[timeoutIndex.Value + 1]) : 30000,
                SubjectNamespace = args[subjectNamespaceIndex.Value + 1]
            };
            NATSConnector natsConnector = new NATSConnector(natsConnectorConfiguration);
            CancellationTokenSource cancellationTokenSource = new CancellationTokenSource();
            var synchronyTask = natsConnector.Start(cancellationTokenSource.Token);
            Console.CancelKeyPress += (sender, e) =>
            {
                cancellationTokenSource.Cancel();
            };
            await synchronyTask;
        }

    }
}
