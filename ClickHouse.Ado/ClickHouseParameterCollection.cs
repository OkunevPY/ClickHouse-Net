using System;
using System.Collections;
using System.Collections.Generic;
using System.Data.Common;
#if !NETCOREAPP11
using System.Data;
#endif
using System.Linq;

namespace ClickHouse.Ado
{
    public class ClickHouseParameterCollection : 
        DbParameterCollection,
        IEnumerable<ClickHouseParameter>
    {
        private readonly List<ClickHouseParameter> InternalList = new List<ClickHouseParameter>();

        // Dictionary lookups for GetValue to improve performance
        private Dictionary<string, int> lookup;
        private Dictionary<string, int> lookupIgnoreCase;

        /// <summary>
        /// Initializes a new instance of the ClickHouseParameterCollection class.
        /// </summary>
        internal ClickHouseParameterCollection()
        {
            InvalidateHashLookups();
        }

        /// <summary>
        /// Invalidate the hash lookup tables.  This should be done any time a change
        /// may throw the lookups out of sync with the list.
        /// </summary>
        internal void InvalidateHashLookups()
        {
            lookup = null;
            lookupIgnoreCase = null;
        }
        public new ClickHouseParameter this[string parameterName]
        {
            get
            {
                int index = IndexOf(parameterName);

                if (index == -1)
                {
                    throw new IndexOutOfRangeException("Parameter not found");
                }

                return this.InternalList[index];
            }
            set
            {
                int index = IndexOf(parameterName);

                if (index == -1)
                {
                    throw new IndexOutOfRangeException("Parameter not found");
                }

                ClickHouseParameter oldValue = this.InternalList[index];

                if (value.CleanName != oldValue.CleanName)
                {
                    InvalidateHashLookups();
                }

                this.InternalList[index] = value;
            }
        }

        /// <summary>
        /// Sync root.
        /// </summary>
        public override object SyncRoot { get { return (InternalList as ICollection).SyncRoot; } }

        public new ClickHouseParameter this[int index]
        {
            get { return this.InternalList[index]; }
            set
            {
                ClickHouseParameter oldValue = this.InternalList[index];

                if (oldValue == value)
                {
                    // Reasigning the same value is a non-op.
                    return;
                }

                if (value.Collection != null)
                {
                    throw new InvalidOperationException("The parameter already belongs to a collection");
                }

                if (value.CleanName != oldValue.CleanName)
                {
                    InvalidateHashLookups();
                }

                this.InternalList[index] = value;
                value.Collection = this;
                oldValue.Collection = null;
            }
        }
        public ClickHouseParameter Add(ClickHouseParameter value)
        {
            // Do not allow parameters without name.
            if (value.Collection != null)
            {
                throw new InvalidOperationException("The parameter already belongs to a collection");
            }

            this.InternalList.Add(value);
            value.Collection = this;
            this.InvalidateHashLookups();

            // Check if there is a name. If not, add a name based in the index of parameter.
            if (value.ParameterName.Trim() == String.Empty || (value.ParameterName.Length == 1 && value.ParameterName[0] == ':'))
            {
                value.ParameterName = ":" + "Parameter" + (IndexOf(value) + 1);
            }

            return value;
        }
        public override void RemoveAt(string parameterName)
        {
            RemoveAt(this.IndexOf(parameterName));
        }
        public override bool Contains(string parameterName)
        {
            return (IndexOf(parameterName) != -1);
        }
        public override int IndexOf(string parameterName)
        {
            int retIndex;
            int scanIndex;

            if ((parameterName[0] == ':') || (parameterName[0] == '@'))
            {
                parameterName = parameterName.Remove(0, 1);
            }

            // Using a dictionary is much faster for 5 or more items            
            if (this.InternalList.Count >= 5)
            {
                if (this.lookup == null)
                {
                    this.lookup = new Dictionary<string, int>();
                    for (scanIndex = 0; scanIndex < this.InternalList.Count; scanIndex++)
                    {
                        var item = this.InternalList[scanIndex];

                        // Store only the first of each distinct value
                        if (!this.lookup.ContainsKey(item.CleanName))
                        {
                            this.lookup.Add(item.CleanName, scanIndex);
                        }
                    }
                }

                // Try to access the case sensitive parameter name first
                if (this.lookup.TryGetValue(parameterName, out retIndex))
                {
                    return retIndex;
                }

                // Case sensitive lookup failed, generate a case insensitive lookup
                if (this.lookupIgnoreCase == null)
                {
                    this.lookupIgnoreCase = new Dictionary<string, int>();
                    for (scanIndex = 0; scanIndex < this.InternalList.Count; scanIndex++)
                    {
                        var item = this.InternalList[scanIndex];

                        // Store only the first of each distinct value
                        if (!this.lookupIgnoreCase.ContainsKey(item.CleanName))
                        {
                            this.lookupIgnoreCase.Add(item.CleanName, scanIndex);
                        }
                    }
                }

                // Then try to access the case insensitive parameter name
                if (this.lookupIgnoreCase.TryGetValue(parameterName, out retIndex))
                {
                    return retIndex;
                }

                return -1;
            }

            retIndex = -1;

            // Scan until a case insensitive match is found, and save its index for possible return.
            // Items that don't match loosely cannot possibly match exactly.
            for (scanIndex = 0; scanIndex < this.InternalList.Count; scanIndex++)
            {
                var item = this.InternalList[scanIndex];

                if (string.Compare(parameterName, item.CleanName) == 0)
                {
                    retIndex = scanIndex;

                    break;
                }
            }

            // Then continue the scan until a case sensitive match is found, and return it.
            // If a case insensitive match was found, it will be re-checked for an exact match.
            for (; scanIndex < this.InternalList.Count; scanIndex++)
            {
                var item = this.InternalList[scanIndex];

                if (item.CleanName == parameterName)
                {
                    return scanIndex;
                }
            }

            // If a case insensitive match was found, it will be returned here.
            return retIndex;
        }
        public override void RemoveAt(int index)
        {
            if (this.InternalList.Count - 1 < index)
            {
                throw new IndexOutOfRangeException();
            }
            Remove(this.InternalList[index]);
        }
        public override void Insert(int index, object oValue)
        {
            CheckType(oValue);
            ClickHouseParameter value = oValue as ClickHouseParameter;
            if (value.Collection != null)
            {
                throw new InvalidOperationException("The parameter already belongs to a collection");
            }

            value.Collection = this;
            this.InternalList.Insert(index, value);
            this.InvalidateHashLookups();
        }
        public void Remove(string parameterName)
        {
            int index = IndexOf(parameterName);
            if (index < 0)
            {
                throw new InvalidOperationException("No parameter with the specified name exists in the collection");
            }
            RemoveAt(index);
        }
        public override void Remove(object oValue)
        {
            CheckType(oValue);
            Remove(oValue as ClickHouseParameter);
        }
        public override bool Contains(object value)
        {
            if (!(value is ClickHouseParameter))
            {
                return false;
            }
            return this.InternalList.Contains((ClickHouseParameter)value);
        }

        /// <summary>
        /// Remove the specified parameter from the collection.
        /// </summary>
        /// <param name="item">Parameter to remove.</param>
        /// <returns>True if the parameter was found and removed, otherwise false.</returns>
        public bool Remove(ClickHouseParameter item)
        {
            if (item == null)
            {
                return false;
            }
            if (item.Collection != this)
            {
                throw new InvalidOperationException("The item does not belong to this collection");
            }
            if (InternalList.Remove(item))
            {
                item.Collection = null;
                this.InvalidateHashLookups();
                return true;
            }
            return false;
        }

        /// <summary>
        /// Convert collection to a System.Array.
        /// </summary>
        /// <param name="array">Destination array.</param>
        /// <param name="arrayIndex">Starting index in destination array.</param>
        public void CopyTo(ClickHouseParameter[] array, int arrayIndex)
        {
            InternalList.CopyTo(array, arrayIndex);
        }

        /// <summary>
        /// Add an Array of parameters to the collection.
        /// </summary>
        /// <param name="values">Parameters to add.</param>
        public override void AddRange(Array values)
        {
            foreach (ClickHouseParameter parameter in values)
            {
                Add(parameter);
            }
        }

        /// <summary>
        /// Get parameter.
        /// </summary>
        /// <param name="parameterName"></param>
        /// <returns></returns>
        protected override DbParameter GetParameter(string parameterName)
        {
            return this[parameterName];
        }

        /// <summary>
        /// Get parameter.
        /// </summary>
        /// <param name="index"></param>
        /// <returns></returns>
        protected override DbParameter GetParameter(int index)
        {
            return this[index];
        }

        /// <summary>
        /// Set parameter.
        /// </summary>
        /// <param name="parameterName"></param>
        /// <param name="value"></param>
        protected override void SetParameter(string parameterName, DbParameter value)
        {
            this[parameterName] = (ClickHouseParameter)value;
        }

        /// <summary>
        /// Set parameter.
        /// </summary>
        /// <param name="index"></param>
        /// <param name="value"></param>
        protected override void SetParameter(int index, DbParameter value)
        {
            this[index] = (ClickHouseParameter)value;
        }

        IEnumerator<ClickHouseParameter> IEnumerable<ClickHouseParameter>.GetEnumerator()
        {
            return InternalList.GetEnumerator();
        }

        /// <summary>
        /// Returns an enumerator that can iterate through the collection.
        /// </summary>
        /// <returns>An <see cref="System.Collections.IEnumerator">IEnumerator</see> that can be used to iterate through the collection.</returns>
        public override IEnumerator GetEnumerator()
        {
            return InternalList.GetEnumerator();
        }
        public override int Count { get { return this.InternalList.Count; } }

        /// <summary>
        /// Copies <see cref="ClickHouseParameter">ClickHouseParameter</see> objects from the <see cref="ClickHouseParameterCollection">ClickHouseParameterCollection</see> to the specified array.
        /// </summary>
        /// <param name="array">An <see cref="System.Array">Array</see> to which to copy the <see cref="ClickHouseParameter">ClickHouseParameter</see> objects in the collection.</param>
        /// <param name="index">The starting index of the array.</param>
        public override void CopyTo(Array array, int index)
        {
            (InternalList as ICollection).CopyTo(array, index);
        }

        /// <summary>
        /// Gets the location of a <see cref="ClickHouseParameter">ClickHouseParameter</see> in the collection.
        /// </summary>
        /// <param name="value">The value of the <see cref="ClickHouseParameter">ClickHouseParameter</see> object to find.</param>
        /// <returns>The zero-based index of the <see cref="ClickHouseParameter">ClickHouseParameter</see> object in the collection.</returns>
        public override int IndexOf(object value)
        {
            CheckType(value);
            return this.InternalList.IndexOf((ClickHouseParameter)value);
        }

        /// <summary>
        /// Adds the specified <see cref="ClickHouseParameter">ClickHouseParameter</see> object to the <see cref="ClickHouseParameterCollection">ClickHouseParameterCollection</see>.
        /// </summary>
        /// <param name="value">The <see cref="ClickHouseParameter">ClickHouseParameter</see> to add to the collection.</param>
        /// <returns>The zero-based index of the new <see cref="ClickHouseParameter">ClickHouseParameter</see> object.</returns>
        public override int Add(object value)
        {
            CheckType(value);
            this.Add((ClickHouseParameter)value);
            return Count - 1;
        }

        /// <summary>
        /// Removes all items from the collection.
        /// </summary>
        public override void Clear()
        {
            foreach (ClickHouseParameter toRemove in this.InternalList)
            {
                // clean up the parameter so it can be added to another command if required.
                toRemove.Collection = null;
            }
            this.InternalList.Clear();
            this.InvalidateHashLookups();
        }

        /// <summary>
        /// In methods taking an object as argument this method is used to verify
        /// that the argument has the type <see cref="ClickHouseParameter">ClickHouseParameter</see>
        /// </summary>
        /// <param name="Object">The object to verify</param>
        private void CheckType(object Object)
        {
            if (!(Object is ClickHouseParameter))
            {
                throw new InvalidCastException(
                    String.Format("Can't cast {0} into ClickHouseParameter", Object.GetType()));
            }
        }
    }
}