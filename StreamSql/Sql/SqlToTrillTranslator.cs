using Microsoft.StreamProcessing;

namespace StreamSql.Sql;

public static class SqlToTrillTranslator
{
    public static IStreamable<Empty, TPayload> ApplyPlan<TPayload>(IStreamable<Empty, TPayload> source, SqlPlan plan)
    {
        // Placeholder: mapping SQL to Trill operators will be expanded in future iterations.
        // For now, we return the source stream unchanged so that the pipeline works end-to-end.
        _ = plan;
        return source;
    }
}
