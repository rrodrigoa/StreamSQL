using System.Linq;
using System.Text;
using ChronosQL.Engine.Sql;

namespace ChronosQL.Engine.Compilation;

internal sealed class QuerySourceGenerator
{
    private readonly SqlPlan _plan;
    private readonly Dictionary<string, FieldReference> _fieldMap = new(StringComparer.Ordinal);
    private readonly Dictionary<string, string> _accessorMap = new(StringComparer.Ordinal);
    private int _accessorIndex;

    public QuerySourceGenerator(SqlPlan plan)
    {
        _plan = plan;
        CollectFields();
    }

    public string Generate()
    {
        var builder = new StringBuilder();
        builder.AppendLine("using System;");
        builder.AppendLine("using System.Collections.Generic;");
        builder.AppendLine("using System.IO;");
        builder.AppendLine("using System.Text.Json;");
        builder.AppendLine("using System.Threading;");
        builder.AppendLine("using System.Threading.Channels;");
        builder.AppendLine("using System.Threading.Tasks;");
        builder.AppendLine("using System.Reactive.Linq;");
        builder.AppendLine("using Microsoft.StreamProcessing;");
        builder.AppendLine("using ChronosQL.Engine;");
        builder.AppendLine();
        builder.AppendLine("namespace ChronosQL.Generated;");
        builder.AppendLine();
        builder.AppendLine("public static class QueryProgram");
        builder.AppendLine("{");
        builder.AppendLine("    public static async Task ExecuteAsync(IAsyncEnumerable<InputEvent> input, ChannelWriter<JsonElement> output, CancellationToken cancellationToken)");
        builder.AppendLine("    {");
        builder.AppendLine("        var observable = CreateObservable(input, cancellationToken);");
        builder.AppendLine("        var stream = observable.ToStreamable();");
        builder.AppendLine("        var projected = stream.Select(payload => Project(payload));");
        builder.AppendLine("        var outputObservable = projected.ToStreamEventObservable();");
        builder.AppendLine("        var dataEvents = outputObservable");
        builder.AppendLine("            .Where(streamEvent => streamEvent.IsData)");
        builder.AppendLine("            .Select(streamEvent => streamEvent.Payload);");
        builder.AppendLine("        var completion = new TaskCompletionSource<object?>(TaskCreationOptions.RunContinuationsAsynchronously);");
        builder.AppendLine("        using var subscription = dataEvents.Subscribe(");
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
        builder.AppendLine("        using var registration = cancellationToken.Register(() =>");
        builder.AppendLine("        {");
        builder.AppendLine("            var exception = new OperationCanceledException(cancellationToken);");
        builder.AppendLine("            output.TryComplete(exception);");
        builder.AppendLine("            completion.TrySetCanceled(cancellationToken);");
        builder.AppendLine("        });");
        builder.AppendLine("        await completion.Task.ConfigureAwait(false);");
        builder.AppendLine("    }");
        builder.AppendLine();
        builder.AppendLine("    private static IObservable<StreamEvent<JsonElement>> CreateObservable(IAsyncEnumerable<InputEvent> input, CancellationToken cancellationToken)");
        builder.AppendLine("        => Observable.Create<StreamEvent<JsonElement>>(async (observer, ct) =>");
        builder.AppendLine("        {");
        builder.AppendLine("            try");
        builder.AppendLine("            {");
        builder.AppendLine("                await foreach (var item in input.WithCancellation(ct))");
        builder.AppendLine("                {");
        builder.AppendLine("                    var timestamp = ResolveTimestamp(item.Payload, item.ArrivalTime);");
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
        builder.AppendLine("    private static JsonElement Project(JsonElement payload)");
        builder.AppendLine("    {");
        builder.AppendLine("        using var stream = new MemoryStream();");
        builder.AppendLine("        using (var writer = new Utf8JsonWriter(stream))");
        builder.AppendLine("        {");
        builder.AppendLine("            writer.WriteStartObject();");
        var fieldIndex = 0;
        foreach (var field in _plan.SelectFields)
        {
            var accessor = GetFieldAccessorName(field.Field);
            var outputName = Escape(field.OutputName);
            builder.AppendLine($"            writer.WritePropertyName(\"{outputName}\");");
            builder.AppendLine($"            if ({accessor}(payload, out var fieldValue{fieldIndex}))");
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
        builder.AppendLine("        using var document = JsonDocument.Parse(stream);");
        builder.AppendLine("        return document.RootElement.Clone();");
        builder.AppendLine("    }");
        builder.AppendLine();
        builder.AppendLine("    private static long ResolveTimestamp(JsonElement payload, long arrivalTime)");
        builder.AppendLine("    {");
        if (_plan.TimestampBy is null)
        {
            builder.AppendLine("        return arrivalTime;");
        }
        else
        {
            var accessor = GetFieldAccessorName(_plan.TimestampBy.Field);
            builder.AppendLine($"        if (!{accessor}(payload, out var timestampElement))");
            builder.AppendLine("        {");
            builder.AppendLine("            throw new InvalidOperationException(\"TIMESTAMP BY field was not found in the payload.\");");
            builder.AppendLine("        }");
            builder.AppendLine("        return ParseTimestamp(timestampElement);");
        }
        builder.AppendLine("    }");
        builder.AppendLine();
        builder.AppendLine("    private static long ParseTimestamp(JsonElement element)");
        builder.AppendLine("    {");
        builder.AppendLine("        switch (element.ValueKind)");
        builder.AppendLine("        {");
        builder.AppendLine("            case JsonValueKind.Number:");
        builder.AppendLine("                if (element.TryGetInt64(out var intValue))");
        builder.AppendLine("                {");
        builder.AppendLine("                    return intValue;");
        builder.AppendLine("                }");
        builder.AppendLine("                return (long)element.GetDouble();");
        builder.AppendLine("            case JsonValueKind.String:");
        builder.AppendLine("                var text = element.GetString();");
        builder.AppendLine("                if (DateTimeOffset.TryParse(text, out var parsed))");
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
        builder.AppendLine("}");
        return builder.ToString();
    }

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
            _accessorMap[key] = $"TryGetField_{_accessorIndex++}";
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
            var escaped = Escape(segment);
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
