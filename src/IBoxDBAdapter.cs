// Copyright (c) Jerry Lee. All rights reserved. Licensed under the MIT License. See LICENSE in the project root for license information.

using System;
using System.Collections.Generic;
using System.Text;
using IBoxDB.LocalServer;

namespace ReSharp.Data.IBoxDB
{
    /// <summary>
    /// An IBoxDBAdapter instance represents a session with the iBoxDB and can be used to query and save instances of your entities. Implements the
    /// <see cref="System.IDisposable" />
    /// </summary>
    /// <seealso cref="System.IDisposable" />
    public class IBoxDBAdapter : IDisposable
    {
        private bool disposed;

        /// <summary>
        /// Initializes a new instance of the <see cref="IBoxDBAdapter" /> class with cache mode.
        /// </summary>
        public IBoxDBAdapter()
        {
            Database = new DB(DB.CacheOnlyArg);
            Database.MinConfig().FileIncSize = 1;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="IBoxDBAdapter" /> class with a database folder path.
        /// </summary>
        /// <param name="dbFolderPath">The database folder path.</param>
        public IBoxDBAdapter(string dbFolderPath)
        {
            Database = new DB(dbFolderPath);
            Database.MinConfig().FileIncSize = 1;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="IBoxDBAdapter" /> class with a database folder path and the raw data of database.
        /// </summary>
        /// <param name="dbFolderPath">The database folder path.</param>
        /// <param name="dbFileRawData">The database file raw data.</param>
        public IBoxDBAdapter(string dbFolderPath, byte[] dbFileRawData)
        {
            DB.Root(dbFolderPath);
            Database = new DB(dbFileRawData);
            Database.MinConfig().FileIncSize = 1;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="IBoxDBAdapter" /> class.
        /// </summary>
        /// <param name="dbFileRawData">The database file raw data.</param>
        public IBoxDBAdapter(byte[] dbFileRawData)
        {
            Database = new DB(dbFileRawData);
            Database.MinConfig().FileIncSize = 1;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="IBoxDBAdapter" /> class.
        /// </summary>
        /// <param name="dbFolderPath">The database folder path.</param>
        /// <param name="localAddress">The local address.</param>
        public IBoxDBAdapter(string dbFolderPath, long localAddress)
        {
            Database = new DB(localAddress, dbFolderPath);
            Database.MinConfig().FileIncSize = 1;
        }

        /// <summary>
        /// Finalizes an instance of the <see cref="IBoxDBAdapter" /> class.
        /// </summary>
        ~IBoxDBAdapter()
        {
            Dispose(false);
        }

        /// <summary>
        /// Gets a value indicating whether the database connection is open.
        /// </summary>
        /// <value><c>true</c> if the database connection is open; otherwise, <c>false</c>.</value>
        public bool IsOpen { get; private set; }

        /// <summary>
        /// Gets the database object of iBoxDB.
        /// </summary>
        public DB Database { get; private set; }

        /// <summary>
        /// Gets the box object of iBoxDB database.
        /// </summary>
        public DB.AutoBox Box { get; private set; }
        
        /// <summary>
        /// Ensures the table is in database. If not exists, create a table.
        /// </summary>
        /// <typeparam name="T">The type of table data.</typeparam>
        /// <param name="tableName">Name of the table.</param>
        /// <param name="names">The property names of table data.</param>
        public void EnsureTable<T>(string tableName, params string[] names) where T : class
        {
            Database.GetConfig().EnsureTable<T>(tableName, names);
        }

        /// <summary>
        /// Opens the database.
        /// </summary>
        /// <exception cref="ObjectDisposedException">database</exception>
        public void Open()
        {
            CheckIfDisposed();

            if (Database == null)
                return;
            
            Box = Database.Open();
            IsOpen = true;
        }

        /// <summary>
        /// Gets the buffer.
        /// </summary>
        /// <returns>System.Byte[].</returns>
        public byte[] GetBuffer() => Database.GetBuffer();

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

            var builder = new StringBuilder();
            builder.AppendFormat("from {0} where ", tableName);
            builder.Append(query);
            return Box.Select<T>(builder.ToString(), arguments);
        }
        
        /// <summary>
        /// Gets the specified object with the <c>tableName</c> and the <c>keys</c>.
        /// </summary>
        /// <typeparam name="T">Specifies the object type of return.</typeparam>
        /// <param name="tableName">Name of the table.</param>
        /// <param name="keys">The keys to locate the specified object.</param>
        /// <returns>The object that was found.</returns>
        public T Get<T>(string tableName, params object[] keys) where T : class, new()
        {
            CheckIfDisposed();
            CheckIfDatabaseIsOpen();
            return Box.Get<T>(tableName, keys);
        }

        /// <summary>
        /// Gets the specified objects with the <c>tableName</c> and the <c>value</c> of <c>key</c>.
        /// </summary>
        /// <typeparam name="T">Specifies the object type of return.</typeparam>
        /// <param name="tableName">Name of the table.</param>
        /// <param name="key">The key to locate the specified object.</param>
        /// <param name="value">The value of key to locate the specified object.</param>
        /// <returns>The objects that was found.</returns>
        public List<T> Get<T>(string tableName, string key, object value) where T : class, new()
        {
            CheckIfDisposed();
            CheckIfDatabaseIsOpen();
            return Box.Select<T>($"from {tableName} where {key} == ?", value);
        }

        /// <summary>
        /// Gets the specified objects with the <c>tableName</c> and multi-conditions.
        /// </summary>
        /// <typeparam name="T">Specifies the object type of return.</typeparam>
        /// <param name="tableName">Name of the table.</param>
        /// <param name="arguments">The arguments.</param>
        /// <param name="logicalOperator">The logical operator.</param>
        /// <returns>The objects that was found.</returns>
        public List<T> Get<T>(string tableName, Dictionary<string, object> arguments, QueryLogicalOperator logicalOperator = QueryLogicalOperator.None) where T : class, new()
        {
            CheckIfDisposed();
            CheckIfDatabaseIsOpen();

            var builder = new StringBuilder();
            builder.AppendFormat("from {0} where ", tableName);

            var i = 0;
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

            var args = new object[arguments.Count];
            arguments.Values.CopyTo(args, 0);
            return Box.Select<T>(builder.ToString(), args);
        }

        /// <summary>
        /// Gets all data in the table.
        /// </summary>
        /// <typeparam name="T">The type of the data in database.</typeparam>
        /// <param name="tableName">Name of the table.</param>
        /// <returns>The data list found in database.</returns>
        public List<T> GetAll<T>(string tableName) where T : class, new()
        {
            CheckIfDisposed();
            CheckIfDatabaseIsOpen();
            return Box.Select<T>($"from {tableName}");
        }
        
        /// <summary>
        /// Inserts data into database.
        /// </summary>
        /// <typeparam name="T">The type of table data.</typeparam>
        /// <param name="tableName">The name of table.</param>
        /// <param name="value">The data need to be inserted.</param>
        /// <returns><c>true</c> if data insert success, <c>false</c> otherwise.</returns>
        public bool Insert<T>(string tableName, T value) where T : class
        {
            CheckIfDisposed();
            CheckIfDatabaseIsOpen();
            return Box.Insert(tableName, value);
        }

        /// <summary>
        /// Inserts data into database.
        /// </summary>
        /// <typeparam name="T">The type of table data.</typeparam>
        /// <param name="value">The data need to be inserted.</param>
        /// <returns><c>true</c> if data insert success, <c>false</c> otherwise.</returns>
        public bool Insert<T>(T value) where T : class
        {
            var tableName = typeof(T).Name;
            return Insert(tableName, value);
        }
        
        /// <summary>
        /// Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
        /// </summary>
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        /// <summary>
        /// Releases unmanaged and - optionally - managed resources.
        /// </summary>
        /// <param name="disposing"><c>true</c> to release both managed and unmanaged resources; <c>false</c> to release only unmanaged resources.</param>
        protected virtual void Dispose(bool disposing)
        {
            if (disposed)
                return;
            
            if (disposing)
            {
                Box?.Dispose();
                Box = null;

                Database?.Dispose();
                Database = null;

                IsOpen = false;
            }

            disposed = true;
        }

        private void CheckIfDatabaseIsOpen()
        {
            if (Box == null)
                throw new NullReferenceException("Database is not open! Before executing database operation, you should invoke method 'Open'.");
        }

        private void CheckIfDisposed()
        {
            if (disposed)
                throw new ObjectDisposedException(nameof(Database));
        }
    }
}