using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Reflection;

namespace Dapper
{
    public static partial class SqlMapper
    {
        /// <summary>
        /// Identity of a cached query in Dapper, used for extensibility.
        /// </summary>
        public class Identity : IEquatable<Identity>
        {
            internal Identity ForGrid(Type primaryType, Type resultType, int gridIndex) =>
                new Identity(sql, commandType, connectionString, primaryType, parametersType, resultType, null, gridIndex);

            internal Identity ForGrid(Type primaryType, Type resultType, Type[] otherTypes, int gridIndex) =>
                new Identity(sql, commandType, connectionString, primaryType, parametersType, resultType, otherTypes, gridIndex);

            /// <summary>
            /// Create an identity for use with DynamicParameters, internal use only.
            /// </summary>
            /// <param name="type">The parameters type to create an <see cref="Identity"/> for.</param>
            /// <returns></returns>
            public Identity ForDynamicParameters(Type type) =>
                new Identity(sql, commandType, connectionString, this.type, type, resultType, null, -1);

            internal Identity(string sql, CommandType? commandType, IDbConnection connection, Type type, Type parametersType, Type resultType, Type[] otherTypes)
                : this(sql, commandType, connection.ConnectionString, type, parametersType, resultType, otherTypes, 0) { /* base call */ }

            private Identity(string sql, CommandType? commandType, string connectionString, Type type, Type parametersType, Type resultType, Type[] otherTypes, int gridIndex)
            {
                this.sql = sql;
                this.commandType = commandType;
                this.connectionString = connectionString;
                this.type = type;
                this.parametersType = parametersType;
                this.gridIndex = gridIndex;
                this.resultType = resultType;
                linkedResultTypes = PrepareLinkedTypes(resultType);
                unchecked
                {
                    hashCode = 17; // we *know* we are using this in a dictionary, so pre-compute this
                    hashCode = (hashCode * 23) + commandType.GetHashCode();
                    hashCode = (hashCode * 23) + gridIndex.GetHashCode();
                    hashCode = (hashCode * 23) + (sql?.GetHashCode() ?? 0);
                    hashCode = (hashCode * 23) + (type?.GetHashCode() ?? 0);
                    if (otherTypes != null)
                    {
                        foreach (var t in otherTypes)
                        {
                            hashCode = (hashCode * 23) + (t?.GetHashCode() ?? 0);
                        }
                    }
                    hashCode = (hashCode * 23) + (connectionString == null ? 0 : connectionStringComparer.GetHashCode(connectionString));
                    hashCode = (hashCode * 23) + (parametersType?.GetHashCode() ?? 0);
                }
            }

            /// <summary>
            /// Whether this <see cref="Identity"/> equals another.
            /// </summary>
            /// <param name="obj">The other <see cref="object"/> to compare to.</param>
            public override bool Equals(object obj) => Equals(obj as Identity);

            /// <summary>
            /// The raw SQL command.
            /// </summary>
            public readonly string sql;

            /// <summary>
            /// The SQL command type.
            /// </summary>
            public readonly CommandType? commandType;

            /// <summary>
            /// The hash code of this Identity.
            /// </summary>
            public readonly int hashCode;

            /// <summary>
            /// The grid index (position in the reader) of this Identity.
            /// </summary>
            public readonly int gridIndex;

            /// <summary>
            /// This <see cref="Type"/> of this Identity.
            /// </summary>
            public readonly Type type;

            /// <summary>
            /// The connection string for this Identity.
            /// </summary>
            public readonly string connectionString;

            /// <summary>
            /// The type of the parameters object for this Identity.
            /// </summary>
            public readonly Type parametersType;

            /// <summary>
            /// Type of query row result.
            /// </summary>
            protected readonly Type resultType;
            /// <summary>
            /// Types linked with query.
            /// </summary>
            public readonly Type[] linkedResultTypes;

            /// <summary>
            /// Gets the hash code for this identity.
            /// </summary>
            /// <returns></returns>
            public override int GetHashCode() => hashCode;

            /// <summary>
            /// Compare 2 Identity objects
            /// </summary>
            /// <param name="other">The other <see cref="Identity"/> object to compare.</param>
            /// <returns>Whether the two are equal</returns>
            public bool Equals(Identity other)
            {
                return other != null
                    && gridIndex == other.gridIndex
                    && type == other.type
                    && sql == other.sql
                    && commandType == other.commandType
                    && connectionStringComparer.Equals(connectionString, other.connectionString)
                    && parametersType == other.parametersType;
            }

            /// <summary>
            /// Analyze type with inner types.
            /// </summary>
            /// <param name="typeForAnalyze">Type for analyze.</param>
            /// <returns>Linked types.</returns>
            protected Type[] PrepareLinkedTypes(Type typeForAnalyze)
            {
                var analyzedTypes = new List<Type>();
                IList<Type> linkedTypesWithoutRoot;
                Type[] returnValue;

                analyzedTypes.Add(typeForAnalyze);
                linkedTypesWithoutRoot = PrepareLinkedTypes(typeForAnalyze, analyzedTypes);
                linkedTypesWithoutRoot.Add(typeForAnalyze);
                returnValue = linkedTypesWithoutRoot.ToArray();

                return returnValue;
            }

            /// <summary>
            /// Analyze type with inner types.
            /// </summary>
            /// <param name="typeForAnalyze">Type for analyze.</param>
            /// <param name="analyzedTypes">Types, which already analyzed.</param>
            /// <returns>Linked types.</returns>
            protected IList<Type> PrepareLinkedTypes(Type typeForAnalyze, List<Type> analyzedTypes)
            {
                BindingFlags requiredFieldsAndPropertiesFlags = BindingFlags.Public |
                             BindingFlags.Instance;
                Type typeForDirectProcessing;
                IList<Type> typesOfFieldsAndProperties;
                IList<Type> returnValue;

                if(typeForAnalyze is IEnumerable)
                {
                    typeForDirectProcessing = typeForAnalyze.GetGenericArguments().SingleOrDefault() ?? typeForAnalyze;
                }
                else
                {
                    typeForDirectProcessing = typeForAnalyze;
                }
                typesOfFieldsAndProperties = typeForAnalyze
                    .GetFields(requiredFieldsAndPropertiesFlags)
                    .Select(curField => curField.FieldType)
                    .Where(curFieldType => !analyzedTypes.Any(curType => curType == curFieldType))
                    .ToList();
                typesOfFieldsAndProperties = typesOfFieldsAndProperties
                    .Union(typeForAnalyze
                    .GetProperties(requiredFieldsAndPropertiesFlags)
                    .Select(curProperty => curProperty.PropertyType)
                    .Where(curPropertyType => (!analyzedTypes.Any(curType => curType == curPropertyType))
                    && (!typesOfFieldsAndProperties.Any(curFieldsAndPropType => curFieldsAndPropType == curPropertyType))))
                    .ToList();

                analyzedTypes.AddRange(typesOfFieldsAndProperties);

                typesOfFieldsAndProperties = typesOfFieldsAndProperties
                    .Union(typesOfFieldsAndProperties.SelectMany(curItem => PrepareLinkedTypes(curItem, analyzedTypes)))
                    .ToList();

                returnValue = typesOfFieldsAndProperties;
                return returnValue;
            }
        }
    }
}
