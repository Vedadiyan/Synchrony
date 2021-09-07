namespace Synchrony.Streaming {
    public class NATSConnectorConfiguration
    {
        public string NATSUrls { get; internal set; }
        public string DbConnectionString { get; internal set; }
        public int Timeout { get; internal set; }
        public string SubjectNamespace { get; internal set; }
    }
}