using System;
using System.Collections.Generic;

namespace Synchrony.Core
{
    public class DeleteEventArgs : EventArgs
    {
        public string TableName { get; }
        public Dictionary<string, object> Record { get; }
        public DeleteEventArgs(string tableName, Dictionary<string, object> record)
        {
            TableName = tableName;
            Record = record;
        }

    }
}