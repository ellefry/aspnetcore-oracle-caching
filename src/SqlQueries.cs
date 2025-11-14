using System.Globalization;

namespace Microsoft.Extensions.Caching.Oracle;

internal sealed class SqlQueries
{
    private const string TableInfoFormat =
    """
        SELECT TABLESPACE_NAME, OWNER, TABLE_NAME, TABLE_TYPE 
        FROM ALL_ALL_TABLES 
        WHERE OWNER = '{0}' 
        AND TABLE_NAME = '{1}'
        """;

    private const string RefreshCacheItemFormat =
        """
        UPDATE
            {0}
        SET "EXPIRESATTIME" =
                (CASE
                     WHEN TO_NUMBER(24 * 60 * 60 * (CAST("ABSOLUTEEXPIRATION" AS DATE) - CAST(:UtcNow AS DATE))) <=
                          "SLIDINGEXPIRATIONINSECONDS"
                         THEN "ABSOLUTEEXPIRATION"
                     ELSE
                         :UtcNow + NUMTODSINTERVAL("SLIDINGEXPIRATIONINSECONDS", 'SECOND')
                    END)
        WHERE "ID" = :Id
          AND :UtcNow <= "EXPIRESATTIME"
          AND "SLIDINGEXPIRATIONINSECONDS" IS NOT NULL
          AND ("ABSOLUTEEXPIRATION" IS NULL OR "ABSOLUTEEXPIRATION" != "EXPIRESATTIME")
        """;

    private const string GetCacheItemFormat =
        """
        SELECT "VALUE"
        FROM {0}
        WHERE "ID" = :Id
          AND :UtcNow <= "EXPIRESATTIME"
        """;

    private const string SetCacheItemFormat =
        """
        BEGIN
            UPDATE {0}
               SET "VALUE"                      = :Value,
                   "EXPIRESATTIME"              = CASE
                                                    WHEN :SlidingExpirationInSeconds IS NULL THEN :AbsoluteExpiration
                                                    ELSE :UtcNow + NUMTODSINTERVAL(:SlidingExpirationInSeconds, 'SECOND')
                                                  END,
                   "SLIDINGEXPIRATIONINSECONDS" = :SlidingExpirationInSeconds,
                   "ABSOLUTEEXPIRATION"         = :AbsoluteExpiration
             WHERE "ID" = :Id;

            IF SQL%ROWCOUNT = 0 THEN
                INSERT INTO {0} (
                    "ID",
                    "VALUE",
                    "EXPIRESATTIME",
                    "SLIDINGEXPIRATIONINSECONDS",
                    "ABSOLUTEEXPIRATION"
                )
                VALUES (
                    :Id,
                    :Value,
                    CASE
                        WHEN :SlidingExpirationInSeconds IS NULL THEN :AbsoluteExpiration
                        ELSE :UtcNow + NUMTODSINTERVAL(:SlidingExpirationInSeconds, 'SECOND')
                    END,
                    :SlidingExpirationInSeconds,
                    :AbsoluteExpiration
                );
            END IF;
        END;
        """;


    private const string DeleteCacheItemFormat =
        """
        DELETE
        FROM {0}
        WHERE "ID" = :Id
        """;

    private const string DeleteExpiredCacheItemsFormat =
        """
        DELETE 
        FROM {0}
        WHERE :UtcNow > "EXPIRESATTIME"
        """;

    public SqlQueries(string schemaName, string tableName)
    {
        var tableNameWithSchema = string.Format(
            CultureInfo.InvariantCulture,
            "{0}.{1}",
            DelimitIdentifier(schemaName), DelimitIdentifier(tableName)
        );

        GetCacheItem = string.Format(CultureInfo.InvariantCulture, GetCacheItemFormat, tableNameWithSchema);

        RefreshCacheItem = string.Format(CultureInfo.InvariantCulture, RefreshCacheItemFormat, tableNameWithSchema);

        DeleteCacheItem = string.Format(CultureInfo.InvariantCulture, DeleteCacheItemFormat, tableNameWithSchema);

        DeleteExpiredCacheItems =
            string.Format(CultureInfo.InvariantCulture, DeleteExpiredCacheItemsFormat, tableNameWithSchema);

        SetCacheItem = string.Format(CultureInfo.InvariantCulture, SetCacheItemFormat, tableNameWithSchema);

        TableInfo = string.Format(CultureInfo.InvariantCulture, TableInfoFormat, EscapeLiteral(schemaName),
            EscapeLiteral(tableName));
    }

    // ReSharper disable once UnusedAutoPropertyAccessor.Global
    public string TableInfo { get; }

    public string GetCacheItem { get; }

    public string RefreshCacheItem { get; }

    public string SetCacheItem { get; }

    public string DeleteCacheItem { get; }

    public string DeleteExpiredCacheItems { get; }

    private static string DelimitIdentifier(string identifier)
    {
        return identifier;
    }

    private static string EscapeLiteral(string literal)
    {
        return literal.Replace("'", "''");
    }
}
