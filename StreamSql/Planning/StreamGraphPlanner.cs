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
        var joinNodes = new List<JoinNodePlan>();
        var unionNodes = new List<UnionNodePlan>();
        var outputNodes = new Dictionary<string, OutputNodePlan>(StringComparer.OrdinalIgnoreCase);
        var outputUsage = new Dictionary<string, OutputUsage>(StringComparer.OrdinalIgnoreCase);

        foreach (var selectDefinition in scriptPlan.SelectStatements)
        {
            var plan = selectDefinition.Plan;
            var outputName = plan.OutputStream ?? CommandLineOptions.DefaultOutputName;
            if (!options.Outputs.ContainsKey(outputName))
            {
                error = $"SELECT {selectDefinition.Index} references unknown output '{outputName}'.";
                return false;
            }

            if (plan.Union is not null)
            {
                if (outputUsage.TryGetValue(outputName, out var usage) && usage.TotalCount > 0)
                {
                    error = $"SELECT {selectDefinition.Index} cannot share output '{outputName}' with a UNION.";
                    return false;
                }

                outputUsage[outputName] = new OutputUsage(1, HasUnion: true);
            }
            else
            {
                if (outputUsage.TryGetValue(outputName, out var usage) && usage.HasUnion)
                {
                    error = $"SELECT {selectDefinition.Index} cannot share output '{outputName}' with a UNION.";
                    return false;
                }

                outputUsage[outputName] = usage.Increment();
            }

            if (!outputNodes.ContainsKey(outputName))
            {
                outputNodes[outputName] = new OutputNodePlan(outputName);
            }

            if (plan.Union is not null)
            {
                var unionNode = new UnionNodePlan($"UNION {selectDefinition.Index}", plan.Union.Distinct);
                unionNodes.Add(unionNode);

                var branchIndex = 0;
                foreach (var branch in plan.Union.Branches)
                {
                    branchIndex++;
                    if (!TryPlanSelectPipeline(
                            branch,
                            selectDefinition.Index,
                            outputName,
                            $" (UNION branch {branchIndex})",
                            withNodes,
                            sourceNodes,
                            options,
                            selectNodes,
                            joinNodes,
                            out var branchSelectNode,
                            out error))
                    {
                        return false;
                    }

                    ConnectNodes(branchSelectNode, unionNode);
                }

                var outputNode = outputNodes[outputName];
                ConnectNodes(unionNode, outputNode);
                continue;
            }

            if (!TryPlanSelectPipeline(
                    plan,
                    selectDefinition.Index,
                    outputName,
                    string.Empty,
                    withNodes,
                    sourceNodes,
                    options,
                    selectNodes,
                    joinNodes,
                    out var selectNode,
                    out error))
            {
                return false;
            }

            var outputForSelect = outputNodes[outputName];
            ConnectNodes(selectNode, outputForSelect);
        }

        foreach (var withNode in withNodes.Values)
        {
            if (withNode.Plan.Union is not null || withNode.Plan.Join is not null || withNode.Plan.Inputs.Count != 1)
            {
                error = $"WITH '{withNode.Name}' must reference a single input.";
                return false;
            }

            var inputName = withNode.Plan.Inputs[0].Name;
            if (string.IsNullOrWhiteSpace(inputName))
            {
                error = $"WITH '{withNode.Name}' does not specify an input.";
                return false;
            }

            if (!TryResolveUpstream(inputName, withNodes, sourceNodes, options, out var upstream, out error))
            {
                error = $"WITH '{withNode.Name}' {error}";
                return false;
            }

            ConnectNodes(upstream, withNode);
        }

        var nodes = sourceNodes.Values.Cast<StreamNodePlan>()
            .Concat(withNodes.Values)
            .Concat(joinNodes)
            .Concat(unionNodes)
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
            joinNodes,
            unionNodes,
            selectNodes,
            outputNodes.Values.ToList(),
            ordered);

        return true;
    }

    private static void ConnectNodes(StreamNodePlan upstream, StreamNodePlan downstream)
    {
        upstream.Downstream.Add(downstream);
        downstream.Upstreams.Add(upstream);

        switch (downstream)
        {
            case OutputNodePlan outputNode:
                outputNode.UpstreamCount++;
                return;
            case UnionNodePlan unionNode:
                unionNode.UpstreamCount++;
                return;
            case JoinNodePlan:
                if (downstream.Upstreams.Count > 2)
                {
                    throw new InvalidOperationException($"Node '{downstream.Name}' has too many upstream sources.");
                }
                return;
            default:
                if (downstream.Upstreams.Count > 1)
                {
                    throw new InvalidOperationException($"Node '{downstream.Name}' has multiple upstream sources.");
                }
                return;
        }
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

    private static bool TryPlanSelectPipeline(
        SqlPlan plan,
        int selectIndex,
        string outputName,
        string labelSuffix,
        IReadOnlyDictionary<string, WithNodePlan> withNodes,
        IDictionary<string, SourceNodePlan> sourceNodes,
        CommandLineOptions options,
        List<SelectNodePlan> selectNodes,
        List<JoinNodePlan> joinNodes,
        out SelectNodePlan selectNode,
        out string? error)
    {
        error = null;
        selectNode = null!;

        if (plan.Union is not null)
        {
            error = $"SELECT {selectIndex}{labelSuffix} cannot contain nested UNION.";
            return false;
        }

        StreamNodePlan upstream;
        if (plan.Join is not null)
        {
            var joinNode = new JoinNodePlan($"JOIN {selectIndex}{labelSuffix}", plan.Join);
            joinNodes.Add(joinNode);

            if (!TryResolveUpstream(plan.Join.LeftSource.Name, withNodes, sourceNodes, options, out var leftUpstream, out error))
            {
                error = $"SELECT {selectIndex}{labelSuffix} {error}";
                return false;
            }

            if (!TryResolveUpstream(plan.Join.RightSource.Name, withNodes, sourceNodes, options, out var rightUpstream, out error))
            {
                error = $"SELECT {selectIndex}{labelSuffix} {error}";
                return false;
            }

            ConnectNodes(leftUpstream, joinNode);
            ConnectNodes(rightUpstream, joinNode);
            upstream = joinNode;
        }
        else
        {
            if (plan.Inputs.Count == 0)
            {
                error = $"SELECT {selectIndex}{labelSuffix} does not specify an input.";
                return false;
            }

            if (plan.Inputs.Count > 1)
            {
                error = $"SELECT {selectIndex}{labelSuffix} references multiple inputs without JOIN.";
                return false;
            }

            if (!TryResolveUpstream(plan.Inputs[0].Name, withNodes, sourceNodes, options, out upstream, out error))
            {
                error = $"SELECT {selectIndex}{labelSuffix} {error}";
                return false;
            }
        }

        selectNode = new SelectNodePlan(selectIndex, outputName, plan);
        selectNodes.Add(selectNode);
        ConnectNodes(upstream, selectNode);
        return true;
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
                case JoinNodePlan joinNode:
                    joinNode.EffectiveTimestamp = null;
                    break;
                case UnionNodePlan unionNode:
                    unionNode.EffectiveTimestamp = null;
                    break;
                case SelectNodePlan selectNode:
                    if (!TryApplyTimestamp(selectNode, selectNode.Plan, out error))
                    {
                        return false;
                    }
                    break;
                case OutputNodePlan outputNode:
                    outputNode.EffectiveTimestamp = outputNode.Upstreams.Count == 1
                        ? outputNode.Upstreams[0].EffectiveTimestamp
                        : null;
                    break;
            }
        }

        return true;
    }

    private static bool TryApplyTimestamp(StreamNodePlan node, SqlPlan plan, out string? error)
    {
        error = null;

        var upstream = node.Upstreams.Count == 1 ? node.Upstreams[0] : null;
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

    private readonly record struct OutputUsage(int TotalCount, bool HasUnion)
    {
        public OutputUsage Increment() => new OutputUsage(TotalCount + 1, HasUnion);
    }
}
