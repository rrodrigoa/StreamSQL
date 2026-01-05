namespace StreamSql.Cli;

public sealed class CommandLineOptions
{
    public string? QueryText { get; init; }
    public string? QueryFilePath { get; init; }
    public bool Follow { get; init; }
    public IReadOnlyDictionary<string, InputSource> Inputs { get; init; } = new Dictionary<string, InputSource>(StringComparer.OrdinalIgnoreCase);
    public IReadOnlyDictionary<string, OutputDestination> Outputs { get; init; } = new Dictionary<string, OutputDestination>(StringComparer.OrdinalIgnoreCase);
    public IReadOnlySet<string> ExplicitInputs { get; init; } = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
    public IReadOnlySet<string> ExplicitOutputs { get; init; } = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
    public IReadOnlySet<string> ImplicitInputs { get; init; } = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
    public IReadOnlySet<string> ImplicitOutputs { get; init; } = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

    public const string DefaultInputName = "input";
    public const string DefaultOutputName = "output";

    public static bool TryParse(string[] args, out CommandLineOptions? options, out string? error)
    {
        options = null;
        error = null;

        if (args.Length == 0)
        {
            error = "Usage: streamsql [--query \"SQL\"] [--file path] [--input name=path|name=-] [--follow] [--out path] [--output name=path|name=-] <query.sql>";
            return false;
        }

        string? queryText = null;
        string? queryFilePath = null;
        var follow = false;
        var inputBindings = new List<InputBinding>();
        var outputBindings = new List<OutputBinding>();
        var explicitInputs = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var explicitOutputs = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        var remaining = new List<string>();

        for (var i = 0; i < args.Length; i++)
        {
            var arg = args[i];
            switch (arg)
            {
                case "--query":
                    if (!TryReadValue(args, ref i, out queryText, out error))
                    {
                        return false;
                    }
                    break;
                case "--file":
                    if (!TryReadValue(args, ref i, out var inputFilePath, out error))
                    {
                        return false;
                    }
                    inputBindings.Add(new InputBinding(DefaultInputName, new InputSource(InputSourceKind.File, inputFilePath)));
                    explicitInputs.Add(DefaultInputName);
                    break;
                case "--input":
                    if (!TryReadValue(args, ref i, out var inputValue, out error))
                    {
                        return false;
                    }

                    if (!TryParseNamedBinding(inputValue, out var inputName, out var inputSource, out error))
                    {
                        return false;
                    }

                    inputBindings.Add(new InputBinding(inputName, inputSource));
                    explicitInputs.Add(inputName);
                    break;
                case "--out":
                    if (!TryReadValue(args, ref i, out var outputFilePath, out error))
                    {
                        return false;
                    }
                    outputBindings.Add(new OutputBinding(DefaultOutputName, new OutputDestination(OutputDestinationKind.File, outputFilePath)));
                    explicitOutputs.Add(DefaultOutputName);
                    break;
                case "--output":
                    if (!TryReadValue(args, ref i, out var outputValue, out error))
                    {
                        return false;
                    }

                    if (!TryParseNamedOutput(outputValue, out var outputName, out var outputDestination, out error))
                    {
                        return false;
                    }

                    outputBindings.Add(new OutputBinding(outputName, outputDestination));
                    explicitOutputs.Add(outputName);
                    break;
                case "--follow":
                    follow = true;
                    break;
                default:
                    remaining.Add(arg);
                    break;
            }
        }

        if (string.IsNullOrWhiteSpace(queryText))
        {
            if (remaining.Count == 0)
            {
                error = "Missing query SQL. Provide --query or the path to a .sql file.";
                return false;
            }

            queryFilePath = remaining.Last();
        }

        if (inputBindings.Count == 0)
        {
            inputBindings.Add(new InputBinding(DefaultInputName, new InputSource(InputSourceKind.Stdin, null)));
        }

        if (outputBindings.Count == 0)
        {
            outputBindings.Add(new OutputBinding(DefaultOutputName, new OutputDestination(OutputDestinationKind.Stdout, null)));
        }

        if (!TryBuildInputMap(inputBindings, out var inputs, out error))
        {
            return false;
        }

        if (!TryBuildOutputMap(outputBindings, out var outputs, out error))
        {
            return false;
        }

        if (follow && !inputs.Values.Any(source => source.Kind == InputSourceKind.File))
        {
            error = "--follow can only be used with file inputs.";
            return false;
        }

        options = new CommandLineOptions
        {
            QueryText = queryText,
            QueryFilePath = queryFilePath,
            Follow = follow,
            Inputs = inputs,
            Outputs = outputs,
            ExplicitInputs = explicitInputs,
            ExplicitOutputs = explicitOutputs,
            ImplicitInputs = BuildImplicitBindings(inputs.Keys, explicitInputs),
            ImplicitOutputs = BuildImplicitBindings(outputs.Keys, explicitOutputs)
        };

        return true;
    }

    private static IReadOnlySet<string> BuildImplicitBindings(
        IEnumerable<string> bindingNames,
        IReadOnlySet<string> explicitBindings)
    {
        var implicitBindings = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (var name in bindingNames)
        {
            if (!explicitBindings.Contains(name))
            {
                implicitBindings.Add(name);
            }
        }

        return implicitBindings;
    }

    private static bool TryReadValue(string[] args, ref int index, out string? value, out string? error)
    {
        error = null;
        value = null;

        if (index + 1 >= args.Length)
        {
            error = $"Missing value for {args[index]}.";
            return false;
        }

        value = args[++index];
        return true;
    }

    private static bool TryParseNamedBinding(
        string value,
        out string name,
        out InputSource source,
        out string? error)
    {
        error = null;
        name = string.Empty;
        source = default!;

        if (!TrySplitNameValue(value, out name, out var rawSource, out error))
        {
            return false;
        }

        if (rawSource == "-")
        {
            source = new InputSource(InputSourceKind.Stdin, null);
            return true;
        }

        source = new InputSource(InputSourceKind.File, rawSource);
        return true;
    }

    private static bool TryParseNamedOutput(
        string value,
        out string name,
        out OutputDestination destination,
        out string? error)
    {
        error = null;
        name = string.Empty;
        destination = default!;

        if (!TrySplitNameValue(value, out name, out var rawDestination, out error))
        {
            return false;
        }

        if (rawDestination == "-")
        {
            destination = new OutputDestination(OutputDestinationKind.Stdout, null);
            return true;
        }

        destination = new OutputDestination(OutputDestinationKind.File, rawDestination);
        return true;
    }

    private static bool TrySplitNameValue(string value, out string name, out string bindingValue, out string? error)
    {
        error = null;
        name = string.Empty;
        bindingValue = string.Empty;

        if (string.IsNullOrWhiteSpace(value))
        {
            error = "Missing name binding.";
            return false;
        }

        var separatorIndex = value.IndexOf('=');
        if (separatorIndex <= 0 || separatorIndex == value.Length - 1)
        {
            error = $"Invalid binding '{value}'. Use name=path or name=- for standard streams.";
            return false;
        }

        name = value[..separatorIndex].Trim();
        bindingValue = value[(separatorIndex + 1)..].Trim();

        if (string.IsNullOrWhiteSpace(name))
        {
            error = $"Invalid binding '{value}'. Name cannot be empty.";
            return false;
        }

        if (string.IsNullOrWhiteSpace(bindingValue))
        {
            error = $"Invalid binding '{value}'. Source cannot be empty.";
            return false;
        }

        return true;
    }

    private static bool TryBuildInputMap(
        IEnumerable<InputBinding> bindings,
        out Dictionary<string, InputSource> inputs,
        out string? error)
    {
        inputs = new Dictionary<string, InputSource>(StringComparer.OrdinalIgnoreCase);
        error = null;
        var stdinCount = 0;

        foreach (var binding in bindings)
        {
            if (inputs.ContainsKey(binding.Name))
            {
                error = $"Duplicate input name '{binding.Name}'.";
                return false;
            }

            if (binding.Source.Kind == InputSourceKind.Stdin)
            {
                stdinCount++;
                if (stdinCount > 1)
                {
                    error = "Only one input may use stdin.";
                    return false;
                }
            }

            inputs[binding.Name] = binding.Source;
        }

        return true;
    }

    private static bool TryBuildOutputMap(
        IEnumerable<OutputBinding> bindings,
        out Dictionary<string, OutputDestination> outputs,
        out string? error)
    {
        outputs = new Dictionary<string, OutputDestination>(StringComparer.OrdinalIgnoreCase);
        error = null;
        var stdoutCount = 0;

        foreach (var binding in bindings)
        {
            if (outputs.ContainsKey(binding.Name))
            {
                error = $"Duplicate output name '{binding.Name}'.";
                return false;
            }

            if (binding.Destination.Kind == OutputDestinationKind.Stdout)
            {
                stdoutCount++;
                if (stdoutCount > 1)
                {
                    error = "Only one output may use stdout.";
                    return false;
                }
            }

            outputs[binding.Name] = binding.Destination;
        }

        return true;
    }
}

public sealed record InputBinding(string Name, InputSource Source);

public sealed record OutputBinding(string Name, OutputDestination Destination);

public readonly record struct InputSource(InputSourceKind Kind, string? Path);

public enum InputSourceKind
{
    File,
    Stdin
}

public readonly record struct OutputDestination(OutputDestinationKind Kind, string? Path);

public enum OutputDestinationKind
{
    File,
    Stdout
}
