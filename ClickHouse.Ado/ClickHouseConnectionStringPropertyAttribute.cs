using System;

namespace ClickHouse.Ado
{
    [AttributeUsage(AttributeTargets.Property)]
    public class ClickHouseConnectionStringPropertyAttribute : Attribute
    {
    }
}