using System.Text.Json;

namespace ChronosQL.Engine.Compilation;

internal sealed class CompiledAggregateBucket
{
    public CompiledAggregateBucket(List<JsonElement> groupValues, IReadOnlyList<CompiledAggregateDefinition> aggregateOrder)
    {
        GroupValues = groupValues;
        AggregateOrder = aggregateOrder.ToList();
        Aggregates = aggregateOrder.Select(definition => new CompiledAggregateAccumulator(definition)).ToList();
    }

    public List<JsonElement> GroupValues { get; }
    public List<CompiledAggregateDefinition> AggregateOrder { get; }
    public List<CompiledAggregateAccumulator> Aggregates { get; }

    public void Accumulate(JsonElement payload)
    {
        foreach (var accumulator in Aggregates)
        {
            accumulator.Accumulate(payload);
        }
    }
}
