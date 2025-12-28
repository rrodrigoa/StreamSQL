namespace StreamSql.Sql;

public sealed record SqlPlan(string RawSql, string? TimestampField);
