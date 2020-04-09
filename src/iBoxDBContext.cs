// Copyright (c) Jerry Lee. All rights reserved. Licensed under the MIT License. See LICENSE in the project root for license information.

using iBoxDB.LocalServer;
using System;
using System.Collections.Generic;
using System.Text;

namespace ReSharp.Data.iBoxDB
{
    /// <summary>
    /// An iBoxDBContext instance represents a session with the iBoxDB and can be used to query and save instances of your entities. Implements the
    /// <see cref="System.IDisposable" />
    /// </summary>
    /// <seealso cref="System.IDisposable" />
    public class iBoxDBContext : IDisposable
    {
        #region Fields

        private DB database;
        private DB.AutoBox box;
        private bool disposedValue = false;

        #endregion Fields

        #region Constructors

        /// <summary>
        /// Initializes a new instance of the <see cref="iBoxDBContext" /> class with cache mode.
        /// </summary>
        public iBoxDBContext()
        {
            database = new DB(DB.CacheOnlyArg);
            database.MinConfig().FileIncSize = 1;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="iBoxDBContext" /> class with a database folder path.
        /// </summary>
        /// <param name="dbFolderPath">The database folder path.</param>
        public iBoxDBContext(string dbFolderPath)
        {
            database = new DB(dbFolderPath);
            database.MinConfig().FileIncSize = 1;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="iBoxDBContext" /> class with a database folder path and the raw data of database.
        /// </summary>
        /// <param name="dbFolderPath">The database folder path.</param>
        /// <param name="dbFileRawData">The database file raw data.</param>
        public iBoxDBContext(string dbFolderPath, byte[] dbFileRawData)
        {
            DB.Root(dbFolderPath);
            database = new DB(dbFileRawData);
            database.MinConfig().FileIncSize = 1;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="iBoxDBContext" /> class.
        /// </summary>
        /// <param name="dbFileRawData">The database file raw data.</param>
        public iBoxDBContext(byte[] dbFileRawData)
        {
            database = new DB(dbFileRawData);
            database.MinConfig().FileIncSize = 1;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="iBoxDBContext" /> class.
        /// </summary>
        /// <param name="dbFolderPath">The database folder path.</param>
        /// <param name="localAddress">The local address.</param>
        public iBoxDBContext(string dbFolderPath, long localAddress)
        {
            database = new DB(localAddress, dbFolderPath);
            database.MinConfig().FileIncSize = 1;
        }

        #endregion Constructors

        #region Destructors

        /// <summary>
        /// Finalizes an instance of the <see cref="iBoxDBContext" /> class.
        /// </summary>
        ~iBoxDBContext()
        {
            Dispose(false);
        }

        #endregion Destructors

        #region Properties

        /// <summary>
        /// Gets a value indicating whether the database connection is open.
        /// </summary>
        /// <value><c>true</c> if the database connection is open; otherwise, <c>false</c>.</value>
        public bool IsOpen
        {
            get;
            private set;
        }

        #endregion Properties

        #region Methods

        /// <summary>
        /// Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
        /// </summary>
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        /// <summary>
        /// Ensures the table is in database. If not exists, create a table.
        /// </summary>
        /// <typeparam name="T">The type of table data.</typeparam>
        /// <param name="tableName">Name of the table.</param>
        /// <param name="names">The property names of table data.</param>
        public void EnsureTable<T>(string tableName, params string[] names) where T : class
        {
            database.GetConfig().EnsureTable<T>(tableName, names);
        }

        /// <summary>
        /// Gets the buffer.
        /// </summary>
        /// <returns>System.Byte[].</returns>
        public byte[] GetBuffer()
        {
            return database.GetBuffer();
        }

        /// <summary>
        /// Inserts data into database.
        /// </summary>
        /// <typeparam name="T">The type of table data.</typeparam>
        /// <param name="tableName">The name of table.</param>
        /// <param name="values">The data need to be inserted.</param>
        /// <returns><c>true</c> if data insert success, <c>false</c> otherwise.</returns>
        public bool Insert<T>(string tableName, params T[] values) where T : class
        {
            CheckIfDisposed();
            CheckIfDatabaseIsOpen();
            return box.Insert(tableName, values);
        }

        /// <summary>
        /// Inserts data into database.
        /// </summary>
        /// <typeparam name="T">The type of table data.</typeparam>
        /// <param name="values">The data need to be inserted.</param>
        /// <returns><c>true</c> if data insert success, <c>false</c> otherwise.</returns>
        public bool Insert<T>(params T[] values) where T : class
        {
            string tableName = typeof(T).Name;
            return Insert(tableName, values);
        }

        /// <summary>
        /// Opens the database.
        /// </summary>
        /// <exception cref="ObjectDisposedException">database</exception>
        public void Open()
        {
            CheckIfDisposed();

            if (database != null)
            {
                box = database.Open();
                IsOpen = true;
            }
        }

        /// <summary>
        /// Queries data with string of specific query language.
        /// </summary>
        /// <typeparam name="T">The type definition of query data.</typeparam>
        /// <param name="tableName">Name of the table.</param>
        /// <param name="query">The string of specific query language.</param>
        /// <param name="arguments">The arguments.</param>
        /// <returns>A <see cref="List{T}" /> that contains query results.</returns>
        public List<T> Query<T>(string tableName, string query, params object[] arguments) where T : class, new()
        {
            CheckIfDisposed();
            CheckIfDatabaseIsOpen();

            StringBuilder builder = new StringBuilder();
            builder.AppendFormat("from {0} where ", tableName);
            builder.Append(query);
            return box.Select<T>(builder.ToString(), arguments);
        }

        /// <summary>
        /// Selects the specified objects with the <c>tableName</c> and the <c>value</c> of <c>key</c>.
        /// </summary>
        /// <typeparam name="T">Specifies the object type of return.</typeparam>
        /// <param name="tableName">Name of the table.</param>
        /// <param name="key">The key to locate the specified object.</param>
        /// <param name="value">The value of key to locate the specified object.</param>
        /// <returns>The objects that was found.</returns>
        public List<T> Select<T>(string tableName, string key, object value) where T : class, new()
        {
            CheckIfDisposed();
            CheckIfDatabaseIsOpen();
            return box.Select<T>($"from {tableName} where {key} == ?", value);
        }

        /// <summary>
        /// Selects the specified objects with the <c>tableName</c> and multi-conditions.
        /// </summary>
        /// <typeparam name="T">Specifies the object type of return.</typeparam>
        /// <param name="tableName">Name of the table.</param>
        /// <param name="arguments">The arguments.</param>
        /// <param name="logicalOperator">The logical operator.</param>
        /// <returns>The objects that was found.</returns>
        public List<T> Select<T>(string tableName, Dictionary<string, object> arguments, QueryLogicalOperator logicalOperator = QueryLogicalOperator.None) where T : class, new()
        {
            CheckIfDisposed();
            CheckIfDatabaseIsOpen();

            StringBuilder builder = new StringBuilder();
            builder.AppendFormat("from {0} where ", tableName);

            int i = 0;
            foreach (var key in arguments.Keys)
            {
                builder.AppendFormat("{0} == ?", key);
                i++;

                if (i < arguments.Keys.Count)
                {
                    switch (logicalOperator)
                    {
                        case QueryLogicalOperator.And:
                        default:
                            builder.Append(" & ");
                            break;

                        case QueryLogicalOperator.Or:
                            builder.Append(" | ");
                            break;
                    }
                }
            }

            object[] args = new object[arguments.Count];
            arguments.Values.CopyTo(args, 0);
            return box.Select<T>(builder.ToString(), args);
        }

        /// <summary>
        /// Get all data in the table.
        /// </summary>
        /// <typeparam name="T">The type of the data in database.</typeparam>
        /// <param name="tableName">Name of the table.</param>
        /// <returns>The data list found in database.</returns>
        public List<T> SelectAll<T>(string tableName) where T : class, new()
        {
            CheckIfDisposed();
            CheckIfDatabaseIsOpen();
            return box.Select<T>(string.Format("from {0}", tableName));
        }

        /// <summary>
        /// Selects the specified object with the <c>tableName</c> and the <c>keys</c>.
        /// </summary>
        /// <typeparam name="T">Specifies the object type of return.</typeparam>
        /// <param name="tableName">Name of the table.</param>
        /// <param name="keys">The keys to locate the specified object.</param>
        /// <returns>The object that was found.</returns>
        public T SelectKey<T>(string tableName, params object[] keys) where T : class, new()
        {
            CheckIfDisposed();
            CheckIfDatabaseIsOpen();
            return box.SelectKey<T>(tableName, keys);
        }

        /// <summary>
        /// Releases unmanaged and - optionally - managed resources.
        /// </summary>
        /// <param name="disposing"><c>true</c> to release both managed and unmanaged resources; <c>false</c> to release only unmanaged resources.</param>
        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {
                    box?.Dispose();
                    box = null;

                    database?.Dispose();
                    database = null;

                    IsOpen = false;
                }

                disposedValue = true;
            }
        }

        private void CheckIfDatabaseIsOpen()
        {
            if (box == null)
            {
                throw new NullReferenceException("Database is not open! Before executing database operation, you should invoke method 'Open'.");
            }
        }

        private void CheckIfDisposed()
        {
            if (disposedValue)
            {
                throw new ObjectDisposedException(nameof(database));
            }
        }

        #endregion Methods
    }
}