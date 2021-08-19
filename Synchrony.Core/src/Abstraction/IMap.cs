namespace Synchrony.Core.Abstraction
{
    public interface IMap
    {
        IMap Map<T>(string name);
        IDTL To<T>(string name);
    }
}