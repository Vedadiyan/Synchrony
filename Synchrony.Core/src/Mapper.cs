using System;
using System.Data;
using Synchrony.Core.Abstraction;

namespace Synchrony.Core
{
    public class Mapper : IMap
    {
        private readonly IDTL dtl;
        internal string FromField { get; private set; }
        internal Type FromType { get; private set; }
        internal string ToField { get; private set; }
        internal Type ToType { get; private set; }
        public Mapper(IDTL dtl)
        {
            this.dtl = dtl;
        }
        public IMap Map<T>(string name)
        {
            FromField = name;
            FromType = typeof(T);
            return this;
        }

        public IDTL To<T>(string name)
        {
            ToField = name;
            ToType = typeof(T);
            return dtl;
        }

    }
}