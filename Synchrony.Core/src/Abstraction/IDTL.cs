using System.Data;
using System.Threading;
using System.Threading.Tasks;

namespace Synchrony.Core.Abstraction
{
    public interface IDTL
    {
        IMap Next { get; }
        Task ApplyAsync(CancellationToken cancellationToken);
        void UseOriginalId(string columnName, SqlDbType sqlDbType);
        DataRow GetInsertedRowFor<T>(T oldId);
    }
}