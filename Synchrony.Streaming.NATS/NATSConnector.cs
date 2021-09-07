using System;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using NATS.Client;
using Synchrony.Core;

namespace Synchrony.Streaming
{
    public class NATSConnector
    {
        private readonly IConnection natsConnection;
        private readonly SynchronyDbContext synchronyDbContext;
        private readonly string subjectNamespace;
        public NATSConnector(NATSConnectorConfiguration natsConnectorConfiguration)
        {
            natsConnection = new ConnectionFactory().CreateConnection(natsConnectorConfiguration.NATSUrls);
            synchronyDbContext = new SynchronyDbContext(natsConnectorConfiguration.DbConnectionString, natsConnectorConfiguration.Timeout);
            synchronyDbContext.Add += onAdd;
            synchronyDbContext.Delete += onDelete;
            synchronyDbContext.Update += onUpdate;
            subjectNamespace = natsConnectorConfiguration.SubjectNamespace;
        }
        private void onAdd(object sender, AddEventArgs e)
        {
            string jsonObject = JsonSerializer.Serialize(e.Record);
            natsConnection.Publish($"{subjectNamespace}.{e.TableName.ToLower()}.add", System.Text.Encoding.UTF8.GetBytes(jsonObject));
        }
        private void onDelete(object sender, DeleteEventArgs e)
        {
            string jsonObject = JsonSerializer.Serialize(e.Record);
            natsConnection.Publish($"{subjectNamespace}.{e.TableName.ToLower()}.delete", System.Text.Encoding.UTF8.GetBytes(jsonObject));
        }
        private void onUpdate(object sender, UpdateEventArgs e)
        {
            string jsonObject = JsonSerializer.Serialize(new
            {
                AfterUpdated = e.NewRecord,
                BeforeUpdated = e.OldRecord
            });
            natsConnection.Publish($"{subjectNamespace}.{e.TableName.ToLower()}.update", System.Text.Encoding.UTF8.GetBytes(jsonObject));
        }
        public async Task Start(CancellationToken cancellationToken)
        {
            EventWaitHandle eventWaitHandle = new EventWaitHandle(false, EventResetMode.ManualReset);
            await synchronyDbContext.Initialize(cancellationToken);
            Timer timer = new Timer(async (x) =>
            {
                await synchronyDbContext.GetChanges(cancellationToken);
            }, null, TimeSpan.Zero, TimeSpan.FromSeconds(2));
            cancellationToken.Register(() =>
            {
                timer.Change(Timeout.Infinite, Timeout.Infinite);
                eventWaitHandle.Reset();
            });
            eventWaitHandle.WaitOne();
        }
    }
}