// Copyright (c) Jerry Lee. All rights reserved. Licensed under the MIT License. See LICENSE in the
// project root for license information.

namespace ReSharp.Data.iBoxDB
{
    /// <summary>
    /// Enum that defines the database query logical operator.
    /// </summary>
    public enum QueryLogicalOperator
    {
        /// <summary>
        /// No query logical operator specified.
        /// </summary>
        None,

        /// <summary>
        /// The query logical operator of And.
        /// </summary>
        And,

        /// <summary>
        /// The query logical operator of Or.
        /// </summary>
        Or
    }
}