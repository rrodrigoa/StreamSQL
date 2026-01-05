using ChronosQL.Engine.Sql;
using StreamSql.Cli;

namespace StreamSql.Planning;

public static class StreamGraphPlanner
{
    public static bool TryPlan(
        ScriptDomPlan scriptPlan,
        CommandLineOptions options,
        out StreamGraphPlan? graphPlan,
        out string? error)
    {
        graphPlan = null;
        error = null;

        var withNodes = new Dictionary<string, WithNodePlan>(StringComparer.OrdinalIgnoreCase);
        foreach (var definition in scriptPlan.WithDefinitions)
        {
            if (string.IsNullOrWhiteSpace(definition.Alias))
            {
                error = "WITH alias name cannot be empty.";
                return false;
            }

            if (withNodes.ContainsKey(definition.Alias))
            {
                error = $"WITH alias '{definition.Alias}' is defined more than once.";
                return false;
            }

            if (!string.IsNullOrWhiteSpace(definition.Plan.OutputStream))
            {
                error = $"WITH '{definition.Alias}' cannot specify INTO.";
                return false;
            }

            withNodes[definition.Alias] = new WithNodePlan(definition.Alias, definition.Plan);
        }

        var sourceNodes = new Dictionary<string, SourceNodePlan>(StringComparer.OrdinalIgnoreCase);
        var selectNodes = new List<SelectNodePlan>();
        var outputNodes = new Dictionary<string, OutputNodePlan>(StringComparer.OrdinalIgnoreCase);

        foreach (var selectDefinition in scriptPlan.SelectStatements)
        {
            var plan = selectDefinition.Plan;
            if (string.IsNullOrWhiteSpace(plan.InputStream))
            {
                error = $"SELECT {selectDefinition.Index} does not specify an input.";
                return false;
            }

            var outputName = plan.OutputStream ?? CommandLineOptions.DefaultOutputName;
            if (!options.Outputs.ContainsKey(outputName))
            {
                error = $"SELECT {selectDefinition.Index} references unknown output '{outputName}'.";
                return false;
            }
            selectNodes.Add(new SelectNodePlan(selectDefinition.Index, outputName, plan));
            if (!outputNodes.ContainsKey(outputName))
            {
                outputNodes[outputName] = new OutputNodePlan(outputName);
            }
        }

        foreach (var withNode in withNodes.Values)
        {
            if (string.IsNullOrWhiteSpace(withNode.Plan.InputStream))
            {
                error = $"WITH '{withNode.Name}' does not specify an input.";
                return false;
            }

            if (!TryResolveUpstream(withNode.Plan.InputStream!, withNodes, sourceNodes, options, out var upstream, out error))
            {
                error = $"WITH '{withNode.Name}' {error}";
                return false;
            }

            ConnectNodes(upstream, withNode);
        }

        foreach (var selectNode in selectNodes)
        {
            if (!TryResolveUpstream(selectNode.Plan.InputStream!, withNodes, sourceNodes, options, out var upstream, out error))
            {
                error = $"SELECT {selectNode.Index} {error}";
                return false;
            }

            ConnectNodes(upstream, selectNode);
            var outputNode = outputNodes[selectNode.OutputName];
            ConnectNodes(selectNode, outputNode);
        }

        var nodes = sourceNodes.Values.Cast<StreamNodePlan>()
            .Concat(withNodes.Values)
            .Concat(selectNodes)
            .Concat(outputNodes.Values)
            .ToList();

        if (!TryTopologicalSort(nodes, out var ordered, out error))
        {
            return false;
        }

        if (!TryValidateTimestamps(ordered, out error))
        {
            return false;
        }

        graphPlan = new StreamGraphPlan(
            nodes,
            sourceNodes.Values.ToList(),
            withNodes.Values.ToList(),
            selectNodes,
            outputNodes.Values.ToList(),
            ordered);

        return true;
    }

    private static void ConnectNodes(StreamNodePlan upstream, StreamNodePlan downstream)
    {
        upstream.Downstream.Add(downstream);
        if (downstream is OutputNodePlan outputNode)
        {
            outputNode.UpstreamCount++;
            return;
        }

        if (downstream.Upstream is not null)
        {
            throw new InvalidOperationException($"Node '{downstream.Name}' has multiple upstream sources.");
        }

        downstream.Upstream = upstream;
    }

    private static bool TryResolveUpstream(
        string name,
        IReadOnlyDictionary<string, WithNodePlan> withNodes,
        IDictionary<string, SourceNodePlan> sourceNodes,
        CommandLineOptions options,
        out StreamNodePlan upstream,
        out string? error)
    {
        error = null;
        upstream = null!;

        if (withNodes.TryGetValue(name, out var withNode))
        {
            upstream = withNode;
            return true;
        }

        if (options.ExplicitInputs.Contains(name))
        {
            upstream = GetSourceNode(name, sourceNodes, options);
            return true;
        }

        if (options.ImplicitInputs.Contains(name))
        {
            upstream = GetSourceNode(name, sourceNodes, options);
            return true;
        }

        error = $"references unknown input '{name}'.";
        return false;
    }

    private static SourceNodePlan GetSourceNode(
        string name,
        IDictionary<string, SourceNodePlan> sourceNodes,
        CommandLineOptions options)
    {
        if (!sourceNodes.TryGetValue(name, out var node))
        {
            var source = options.Inputs[name];
            node = new SourceNodePlan(name, source);
            sourceNodes.Add(name, node);
        }

        return node;
    }

    private static bool TryTopologicalSort(
        IReadOnlyList<StreamNodePlan> nodes,
        out List<StreamNodePlan> ordered,
        out string? error)
    {
        ordered = new List<StreamNodePlan>(nodes.Count);
        error = null;

        var indegree = nodes.ToDictionary(node => node, node => 0);
        foreach (var node in nodes)
        {
            foreach (var downstream in node.Downstream)
            {
                indegree[downstream]++;
            }
        }

        var queue = new Queue<StreamNodePlan>(indegree.Where(pair => pair.Value == 0).Select(pair => pair.Key));
        while (queue.Count > 0)
        {
            var node = queue.Dequeue();
            ordered.Add(node);

            foreach (var downstream in node.Downstream)
            {
                indegree[downstream]--;
                if (indegree[downstream] == 0)
                {
                    queue.Enqueue(downstream);
                }
            }
        }

        if (ordered.Count != nodes.Count)
        {
            var withCycle = nodes.OfType<WithNodePlan>().FirstOrDefault(node => indegree[node] > 0);
            error = withCycle is null
                ? "WITH definitions contain a cycle."
                : $"WITH definitions contain a cycle involving '{withCycle.Name}'.";
            return false;
        }

        return true;
    }

    private static bool TryValidateTimestamps(
        IReadOnlyList<StreamNodePlan> ordered,
        out string? error)
    {
        error = null;

        foreach (var node in ordered)
        {
            switch (node)
            {
                case SourceNodePlan:
                    node.EffectiveTimestamp = null;
                    break;
                case WithNodePlan withNode:
                    if (!TryApplyTimestamp(withNode, withNode.Plan, out error))
                    {
                        return false;
                    }
                    break;
                case SelectNodePlan selectNode:
                    if (!TryApplyTimestamp(selectNode, selectNode.Plan, out error))
                    {
                        return false;
                    }
                    break;
                case OutputNodePlan outputNode:
                    outputNode.EffectiveTimestamp = outputNode.Upstream?.EffectiveTimestamp;
                    break;
            }
        }

        return true;
    }

    private static bool TryApplyTimestamp(StreamNodePlan node, SqlPlan plan, out string? error)
    {
        error = null;

        var upstream = node.Upstream;
        var upstreamTimestamp = upstream?.EffectiveTimestamp;
        var label = node is WithNodePlan ? $"WITH '{node.Name}'" : node.Name;
        if (plan.TimestampBy is not null)
        {
            if (upstreamTimestamp is not null)
            {
                error = $"{label} cannot define TIMESTAMP BY because upstream stream already has a timestamp.";
                return false;
            }

            if (upstream is WithNodePlan withUpstream && withUpstream.Downstream.Count > 1)
            {
                error = $"{label} cannot define TIMESTAMP BY because upstream stream fans out to multiple consumers.";
                return false;
            }

            node.EffectiveTimestamp = plan.TimestampBy;
            return true;
        }

        node.EffectiveTimestamp = upstreamTimestamp;
        return true;
    }
}
