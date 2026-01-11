using System.Text;
using ChronosQL.Engine.Sql;

namespace ChronosQL.Engine.Compilation;

internal sealed class ScriptQuerySourceGenerator
{
    private readonly SqlScriptPlan _plan;

    public ScriptQuerySourceGenerator(SqlScriptPlan plan)
    {
        _plan = plan;
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
        builder.AppendLine("    public static async Task ExecuteAsync(IReadOnlyDictionary<string, IAsyncEnumerable<InputEvent>> inputs, IReadOnlyDictionary<string, ChannelWriter<JsonElement>> outputs, CancellationToken cancellationToken)");
        builder.AppendLine("    {");
        builder.AppendLine("        var tasks = new List<Task>();");
        for (var i = 0; i < _plan.Statements.Count; i++)
        {
            var statement = _plan.Statements[i];
            var prefix = $"Select{i}";
            var planGenerator = new QueryPlanSourceGenerator(statement, prefix);
            var inputName = $"input_{prefix}";
            var outputName = $"output_{prefix}";
            builder.AppendLine($"        if (!inputs.TryGetValue(\"{Escape(statement.InputName)}\", out var {inputName}))");
            builder.AppendLine("        {");
            builder.AppendLine($"            throw new InvalidOperationException(\"SELECT references unknown input '{Escape(statement.InputName)}'.\");");
            builder.AppendLine("        }");
            builder.AppendLine($"        if (!outputs.TryGetValue(\"{Escape(statement.OutputName)}\", out var {outputName}))");
            builder.AppendLine("        {");
            builder.AppendLine($"            throw new InvalidOperationException(\"SELECT references unknown output '{Escape(statement.OutputName)}'.\");");
            builder.AppendLine("        }");
            builder.AppendLine($"        tasks.Add({planGenerator.ExecuteMethodName}({inputName}, {outputName}, cancellationToken));");
        }
        builder.AppendLine("        await Task.WhenAll(tasks).ConfigureAwait(false);");
        builder.AppendLine("    }");
        builder.AppendLine();

        for (var i = 0; i < _plan.Statements.Count; i++)
        {
            var statement = _plan.Statements[i];
            var prefix = $"Select{i}";
            var planGenerator = new QueryPlanSourceGenerator(statement, prefix);
            planGenerator.Generate(builder);
        }

        builder.AppendLine("}");
        return builder.ToString();
    }

    private static string Escape(string value) => value.Replace("\\", "\\\\").Replace("\"", "\\\"");
}
