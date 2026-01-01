using System.Text.Json;
using System.Collections.Generic;
using System.Linq;
using ChronosQL.Engine.Sql;

namespace ChronosQL.Engine;

internal sealed class AggregateBucket
{
    public AggregateBucket(List<JsonElement> groupValues, IReadOnlyList<AggregateDefinition> aggregateOrder)
    {
        GroupValues = groupValues;
        AggregateOrder = aggregateOrder.ToList();
        Aggregates = aggregateOrder.Select(definition => new AggregateAccumulator(definition)).ToList();
    }

    public List<JsonElement> GroupValues { get; }
    public List<AggregateDefinition> AggregateOrder { get; }
    public List<AggregateAccumulator> Aggregates { get; }

    public void Accumulate(JsonElement payload)
    {
        foreach (var accumulator in Aggregates)
        {
            accumulator.Accumulate(payload);
        }
    }
}
