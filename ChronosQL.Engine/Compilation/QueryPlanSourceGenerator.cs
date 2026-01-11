using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using ChronosQL.Engine.Sql;

namespace ChronosQL.Engine.Compilation;

internal sealed class QueryPlanSourceGenerator
{
    private readonly SqlPlan _plan;
    private readonly string _prefix;
    private readonly Dictionary<string, FieldReference> _fieldMap = new(StringComparer.Ordinal);
    private readonly Dictionary<string, string> _accessorMap = new(StringComparer.Ordinal);
    private int _accessorIndex;

    public QueryPlanSourceGenerator(SqlPlan plan, string prefix)
    {
        _plan = plan;
        _prefix = prefix;
        CollectFields();
    }

    public string ExecuteMethodName => $"ExecuteAsync_{_prefix}";

    public void Generate(StringBuilder builder)
    {
        var createObservableName = Name("CreateObservable");
        var projectName = Name("Project");
        var resolveTimestampName = Name("ResolveTimestamp");
        var parseTimestampName = Name("ParseTimestamp");
        var executeName = ExecuteMethodName;

        builder.AppendLine($"    private static async Task {executeName}(IAsyncEnumerable<InputEvent> input, ChannelWriter<JsonElement> output, CancellationToken cancellationToken)");
        builder.AppendLine("    {");
        builder.AppendLine($"        IObservable<StreamEvent<JsonElement>> observable = {createObservableName}(input, cancellationToken);");
        builder.AppendLine("        IStreamable<Empty, JsonElement> stream = observable.ToStreamable();");
        builder.AppendLine($"        IStreamable<Empty, JsonElement> projected = stream.Select(payload => {projectName}(payload));");
        builder.AppendLine("        IObservable<StreamEvent<JsonElement>> outputObservable = projected.ToStreamEventObservable();");
        builder.AppendLine("        IObservable<JsonElement> dataEvents = outputObservable");
        builder.AppendLine("            .Where(streamEvent => streamEvent.IsData)");
        builder.AppendLine("            .Select(streamEvent => streamEvent.Payload);");
        builder.AppendLine("        TaskCompletionSource<object?> completion = new(TaskCreationOptions.RunContinuationsAsynchronously);");
        builder.AppendLine("        using IDisposable subscription = dataEvents.Subscribe(");
        builder.AppendLine("            onNext: payload =>");
        builder.AppendLine("            {");
        builder.AppendLine("                output.TryWrite(payload);");
        builder.AppendLine("            },");
        builder.AppendLine("            onError: ex =>");
        builder.AppendLine("            {");
        builder.AppendLine("                output.TryComplete(ex);");
        builder.AppendLine("                completion.TrySetException(ex);");
        builder.AppendLine("            },");
        builder.AppendLine("            onCompleted: () =>");
        builder.AppendLine("            {");
        builder.AppendLine("                output.TryComplete();");
        builder.AppendLine("                completion.TrySetResult(null);");
        builder.AppendLine("            });");
        builder.AppendLine("        using CancellationTokenRegistration registration = cancellationToken.Register(() =>");
        builder.AppendLine("        {");
        builder.AppendLine("            OperationCanceledException exception = new(cancellationToken);");
        builder.AppendLine("            output.TryComplete(exception);");
        builder.AppendLine("            completion.TrySetCanceled(cancellationToken);");
        builder.AppendLine("        });");
        builder.AppendLine("        await completion.Task.ConfigureAwait(false);");
        builder.AppendLine("    }");
        builder.AppendLine();
        builder.AppendLine($"    private static IObservable<StreamEvent<JsonElement>> {createObservableName}(IAsyncEnumerable<InputEvent> input, CancellationToken cancellationToken)");
        builder.AppendLine("        => Observable.Create<StreamEvent<JsonElement>>(async (observer, ct) =>");
        builder.AppendLine("        {");
        builder.AppendLine("            try");
        builder.AppendLine("            {");
        builder.AppendLine("                await foreach (InputEvent item in input.WithCancellation(ct))");
        builder.AppendLine("                {");
        builder.AppendLine($"                    long timestamp = {resolveTimestampName}(item.Payload, item.ArrivalTime);");
        builder.AppendLine("                    observer.OnNext(StreamEvent.CreatePoint(timestamp, item.Payload));");
        builder.AppendLine("                }");
        builder.AppendLine();
        builder.AppendLine("                observer.OnNext(StreamEvent.CreatePunctuation<JsonElement>(StreamEvent.InfinitySyncTime));");
        builder.AppendLine("                observer.OnCompleted();");
        builder.AppendLine("            }");
        builder.AppendLine("            catch (Exception ex)");
        builder.AppendLine("            {");
        builder.AppendLine("                observer.OnError(ex);");
        builder.AppendLine("            }");
        builder.AppendLine("        });");
        builder.AppendLine();
        builder.AppendLine($"    private static JsonElement {projectName}(JsonElement payload)");
        builder.AppendLine("    {");
        if (_plan.SelectAll)
        {
            builder.AppendLine("        return payload;");
        }
        else
        {
            builder.AppendLine("        using MemoryStream stream = new();");
            builder.AppendLine("        using (Utf8JsonWriter writer = new(stream))");
            builder.AppendLine("        {");
            builder.AppendLine("            writer.WriteStartObject();");
            var fieldIndex = 0;
            foreach (var field in _plan.SelectFields)
            {
                string accessor = GetFieldAccessorName(field.Field);
                string outputName = Escape(field.OutputName);
                builder.AppendLine($"            writer.WritePropertyName(\"{outputName}\");");
                builder.AppendLine($"            if ({accessor}(payload, out JsonElement fieldValue{fieldIndex}))");
                builder.AppendLine("            {");
                builder.AppendLine($"                fieldValue{fieldIndex}.WriteTo(writer);");
                builder.AppendLine("            }");
                builder.AppendLine("            else");
                builder.AppendLine("            {");
                builder.AppendLine("                writer.WriteNullValue();");
                builder.AppendLine("            }");
                fieldIndex++;
            }
            builder.AppendLine("            writer.WriteEndObject();");
            builder.AppendLine("        }");
            builder.AppendLine("        stream.Position = 0;");
            builder.AppendLine("        using JsonDocument document = JsonDocument.Parse(stream);");
            builder.AppendLine("        return document.RootElement.Clone();");
        }
        builder.AppendLine("    }");
        builder.AppendLine();
        builder.AppendLine($"    private static long {resolveTimestampName}(JsonElement payload, long arrivalTime)");
        builder.AppendLine("    {");
        if (_plan.TimestampBy is null)
        {
            builder.AppendLine("        return arrivalTime;");
        }
        else
        {
            string accessor = GetFieldAccessorName(_plan.TimestampBy.Field);
            builder.AppendLine($"        if (!{accessor}(payload, out JsonElement timestampElement))");
            builder.AppendLine("        {");
            builder.AppendLine("            throw new InvalidOperationException(\"TIMESTAMP BY field was not found in the payload.\");");
            builder.AppendLine("        }");
            builder.AppendLine($"        return {parseTimestampName}(timestampElement);");
        }
        builder.AppendLine("    }");
        builder.AppendLine();
        builder.AppendLine($"    private static long {parseTimestampName}(JsonElement element)");
        builder.AppendLine("    {");
        builder.AppendLine("        switch (element.ValueKind)");
        builder.AppendLine("        {");
        builder.AppendLine("            case JsonValueKind.Number:");
        builder.AppendLine("                if (element.TryGetInt64(out long intValue))");
        builder.AppendLine("                {");
        builder.AppendLine("                    return intValue;");
        builder.AppendLine("                }");
        builder.AppendLine("                return (long)element.GetDouble();");
        builder.AppendLine("            case JsonValueKind.String:");
        builder.AppendLine("                string? text = element.GetString();");
        builder.AppendLine("                if (DateTimeOffset.TryParse(text, out DateTimeOffset parsed))");
        builder.AppendLine("                {");
        builder.AppendLine("                    return parsed.ToUnixTimeMilliseconds();");
        builder.AppendLine("                }");
        builder.AppendLine("                break;");
        builder.AppendLine("        }");
        builder.AppendLine("        throw new InvalidOperationException(\"TIMESTAMP BY requires a numeric or ISO-8601 string value.\");");
        builder.AppendLine("    }");
        builder.AppendLine();
        foreach (var entry in _fieldMap)
        {
            BuildFieldAccessor(builder, entry.Key, entry.Value);
        }
    }

    private string Name(string baseName) => $"{baseName}_{_prefix}";

    private void CollectFields()
    {
        foreach (var field in _plan.SelectFields.Select(item => item.Field))
        {
            AddField(field);
        }

        if (_plan.TimestampBy is not null)
        {
            AddField(_plan.TimestampBy.Field);
        }
    }

    private void AddField(FieldReference field)
    {
        var key = string.Join(".", field.PathSegments);
        if (!_fieldMap.ContainsKey(key))
        {
            _fieldMap[key] = field;
            _accessorMap[key] = $"TryGetField_{_prefix}_{_accessorIndex++}";
        }
    }

    private static string Escape(string value) => value.Replace("\\", "\\\\").Replace("\"", "\\\"");

    private string GetFieldAccessorName(FieldReference field)
    {
        var key = string.Join(".", field.PathSegments);
        return _accessorMap[key];
    }

    private void BuildFieldAccessor(StringBuilder builder, string key, FieldReference field)
    {
        var accessorName = _accessorMap[key];
        builder.AppendLine($"    private static bool {accessorName}(JsonElement payload, out JsonElement value)");
        builder.AppendLine("    {");
        builder.AppendLine("        value = payload;");
        foreach (var segment in field.PathSegments)
        {
            string escaped = Escape(segment);
            builder.AppendLine("        if (value.ValueKind != JsonValueKind.Object)");
            builder.AppendLine("        {");
            builder.AppendLine("            return false;");
            builder.AppendLine("        }");
            builder.AppendLine($"        if (!value.TryGetProperty(\"{escaped}\", out value))");
            builder.AppendLine("        {");
            builder.AppendLine("            return false;");
            builder.AppendLine("        }");
        }
        builder.AppendLine("        return true;");
        builder.AppendLine("    }");
        builder.AppendLine();
    }
}
