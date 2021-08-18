using System;
using System.Collections.Generic;

namespace Synchrony.Core
{
    public class UpdateEventArgs : EventArgs
    {
        public string TableName { get; }
        public Dictionary<string, object> OldRecord { get; }
        public Dictionary<string, object> NewRecord { get; }
        public UpdateEventArgs(string tableName, Dictionary<string, object> oldRecord, Dictionary<string, object> newRecord)
        {
            TableName = tableName;
            OldRecord = oldRecord;
            NewRecord = newRecord;
        }
    }
}