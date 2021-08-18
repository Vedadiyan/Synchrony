using System;
using System.Collections.Generic;

namespace Synchrony.Core
{
    public class AddEventArgs : EventArgs
    {
        public string TableName { get; }
        public Dictionary<string, object> Record { get; }
        public AddEventArgs(string tableName, Dictionary<string, object> record)
        {
            TableName = tableName;
            Record = record;
        }
    }
}