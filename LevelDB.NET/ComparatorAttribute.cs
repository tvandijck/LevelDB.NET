using System;

namespace LevelDB.NET
{
    public sealed class ComparatorAttribute : Attribute
    {
        public ComparatorAttribute(string name)
        {
            Name = name;
        }

        public string Name { get; }
    }
}
