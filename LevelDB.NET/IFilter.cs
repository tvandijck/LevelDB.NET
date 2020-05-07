namespace LevelDB.NET
{
    public interface IFilter
    {
        int Compare(byte[] key);
    }
}
