using ChronosQL.Engine.Sql;
using StreamSql.Cli;

namespace StreamSql.Planning;

public enum StreamNodeKind
{
    Source,
    With,
    Join,
    Union,
    Select,
    Output
}

public abstract class StreamNodePlan
{
    protected StreamNodePlan(string name, StreamNodeKind kind)
    {
        Name = name;
        Kind = kind;
    }

    public string Name { get; }
    public StreamNodeKind Kind { get; }
    public List<StreamNodePlan> Upstreams { get; } = new();
    public List<StreamNodePlan> Downstream { get; } = new();
    public TimestampByDefinition? EffectiveTimestamp { get; internal set; }
}

public sealed class SourceNodePlan : StreamNodePlan
{
    public SourceNodePlan(string name, InputSource source)
        : base(name, StreamNodeKind.Source)
    {
        Source = source;
    }

    public InputSource Source { get; }
}

public sealed class WithNodePlan : StreamNodePlan
{
    public WithNodePlan(string name, SqlPlan plan)
        : base(name, StreamNodeKind.With)
    {
        Plan = plan;
    }

    public SqlPlan Plan { get; }
}

public sealed class SelectNodePlan : StreamNodePlan
{
    public SelectNodePlan(int index, string outputName, SqlPlan plan)
        : base($"SELECT {index}", StreamNodeKind.Select)
    {
        Index = index;
        OutputName = outputName;
        Plan = plan;
    }

    public int Index { get; }
    public string OutputName { get; }
    public SqlPlan Plan { get; }
}

public sealed class JoinNodePlan : StreamNodePlan
{
    public JoinNodePlan(string name, JoinDefinition join)
        : base(name, StreamNodeKind.Join)
    {
        Join = join;
    }

    public JoinDefinition Join { get; }
}

public sealed class UnionNodePlan : StreamNodePlan
{
    public UnionNodePlan(string name, bool distinct)
        : base(name, StreamNodeKind.Union)
    {
        Distinct = distinct;
    }

    public bool Distinct { get; }
    public int UpstreamCount { get; internal set; }
}

public sealed class OutputNodePlan : StreamNodePlan
{
    public OutputNodePlan(string name)
        : base(name, StreamNodeKind.Output)
    {
    }

    public int UpstreamCount { get; internal set; }
}

public sealed record StreamGraphPlan(
    IReadOnlyList<StreamNodePlan> Nodes,
    IReadOnlyList<SourceNodePlan> Sources,
    IReadOnlyList<WithNodePlan> WithNodes,
    IReadOnlyList<JoinNodePlan> JoinNodes,
    IReadOnlyList<UnionNodePlan> UnionNodes,
    IReadOnlyList<SelectNodePlan> SelectNodes,
    IReadOnlyList<OutputNodePlan> OutputNodes,
    IReadOnlyList<StreamNodePlan> TopologicalOrder);
