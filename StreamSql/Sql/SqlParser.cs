using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace StreamSql.Sql;

public static class SqlParser
{
    public static SqlPlan Parse(string sql)
    {
        var parser = new TSql150Parser(false);
        var fragment = parser.Parse(new StringReader(sql), out var errors);

        if (errors is { Count: > 0 })
        {
            var message = string.Join(Environment.NewLine, errors.Select(e => e.Message));
            throw new InvalidOperationException($"SQL parse error:{Environment.NewLine}{message}");
        }

        var visitor = new SqlValidationVisitor();
        fragment.Accept(visitor);

        if (visitor.Unsupported.Count > 0)
        {
            var details = string.Join(", ", visitor.Unsupported);
            throw new InvalidOperationException($"Unsupported SQL syntax detected: {details}");
        }

        return new SqlPlan(sql, visitor.TimestampField);
    }

    private sealed class SqlValidationVisitor : TSqlFragmentVisitor
    {
        public List<string> Unsupported { get; } = new();
        public string? TimestampField { get; private set; }

        public override void ExplicitVisit(QuerySpecification node)
        {
            base.ExplicitVisit(node);
            if (node.SelectElements is null || node.SelectElements.Count == 0)
            {
                Unsupported.Add("SELECT list");
            }
        }

        public override void ExplicitVisit(FromClause node)
        {
            base.ExplicitVisit(node);
            if (node.TableReferences.Count == 0)
            {
                Unsupported.Add("FROM");
            }
        }

        public override void ExplicitVisit(SelectStatement node)
        {
            base.ExplicitVisit(node);
            if (node.QueryExpression is not QuerySpecification)
            {
                Unsupported.Add("Only simple SELECT statements are supported");
            }
        }

        public override void ExplicitVisit(TimestampByClause node)
        {
            if (node.Expression is ColumnReferenceExpression column)
            {
                TimestampField = string.Join('.', column.MultiPartIdentifier.Identifiers.Select(id => id.Value));
            }
        }

        public override void ExplicitVisit(TumblingWindowClause node)
        {
            base.ExplicitVisit(node);
        }
    }
}
