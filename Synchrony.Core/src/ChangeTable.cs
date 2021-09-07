namespace Synchrony.Core
{
    public class ChangeTable
    {
        public int ObjectId { get; set; }
        public int Version { get; set; }
        public int SourceObjectId { get; set; }
        public string CaptureInstance { get; set; }
        public string RoleName { get; set; }
        public string[] Columns { get; set; }
    }
}