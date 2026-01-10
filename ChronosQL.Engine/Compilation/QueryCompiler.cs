using System.Reflection;
using System.Text.Json;
using System.Threading.Channels;
using ChronosQL.Engine.Sql;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.StreamProcessing;
using System.Reactive.Linq;

namespace ChronosQL.Engine.Compilation;

public sealed class QueryCompiler
{
    private readonly IReadOnlyList<MetadataReference> _references;

    public QueryCompiler()
    {
        _references = BuildMetadataReferences();
    }

    public CompiledQuery Compile(SqlPlan plan)
    {
        var generator = new QuerySourceGenerator(plan);
        var source = generator.Generate();

        var syntaxTree = CSharpSyntaxTree.ParseText(source);
        var compilation = CSharpCompilation.Create(
                assemblyName: $"ChronosQL.Generated.{Guid.NewGuid():N}",
                syntaxTrees: new[] { syntaxTree },
                references: _references,
                options: new CSharpCompilationOptions(OutputKind.DynamicallyLinkedLibrary))
            .WithOptimizationLevel(OptimizationLevel.Release);

        using var assemblyStream = new MemoryStream();
        var emitResult = compilation.Emit(assemblyStream);
        if (!emitResult.Success)
        {
            var diagnostics = string.Join(Environment.NewLine, emitResult.Diagnostics.Select(diagnostic => diagnostic.ToString()));
            throw new InvalidOperationException($"Query compilation failed:{Environment.NewLine}{diagnostics}");
        }

        assemblyStream.Position = 0;
        var assembly = Assembly.Load(assemblyStream.ToArray());
        var type = assembly.GetType("ChronosQL.Generated.QueryProgram")
                   ?? throw new InvalidOperationException("Compiled query did not contain QueryProgram type.");
        var method = type.GetMethod("ExecuteAsync", BindingFlags.Public | BindingFlags.Static)
                     ?? throw new InvalidOperationException("Compiled query did not contain ExecuteAsync method.");

        var runner = (Func<IAsyncEnumerable<InputEvent>, ChannelWriter<JsonElement>, CancellationToken, Task>)method
            .CreateDelegate(typeof(Func<IAsyncEnumerable<InputEvent>, ChannelWriter<JsonElement>, CancellationToken, Task>));

        return new CompiledQuery(runner, source);
    }

    private static IReadOnlyList<MetadataReference> BuildMetadataReferences()
    {
        var assemblies = new[]
        {
            typeof(object).Assembly,
            typeof(Enumerable).Assembly,
            typeof(JsonElement).Assembly,
            typeof(InputEvent).Assembly,
            typeof(ChannelWriter<>).Assembly,
            typeof(StreamEvent<>).Assembly,
            typeof(Observable).Assembly
        };

        var references = new List<MetadataReference>();
        foreach (var assembly in assemblies.Concat(AppDomain.CurrentDomain.GetAssemblies()))
        {
            if (assembly.IsDynamic)
            {
                continue;
            }

            var location = assembly.Location;
            if (string.IsNullOrWhiteSpace(location))
            {
                continue;
            }

            if (references.Any(reference => string.Equals(reference.Display, location, StringComparison.OrdinalIgnoreCase)))
            {
                continue;
            }

            references.Add(MetadataReference.CreateFromFile(location));
        }

        return references;
    }
}
