using Microsoft.SqlServer.TransactSql.ScriptDom;

// TODO: Exclude temp tables from all checks - There doesn't appear to be a programmatic way to identify temp tables other than "starts with #"
// TODO: Reorganize methods into better classes instead of lumping everything together or making everything static
// TODO: Change hardcoded file path to use input parameter
// TODO: Expand to accpet text directly? Or stick with file paths? - Would support piping OBJECTDEFINITION direct from SQL
// TODO: Consider converting into compiled PowerShell cmdlet to support piping natively?
// TODO: Test nested tables in update/delete statements - for example UPDATE x SET x.Col = 1 FROM (SELECT col FROM dbo.MyTable t WHERE t.ID < 100) x

class Program
{
    static void Main(string[] args)
    {
        if (args.Length == 0)
        {
            Console.WriteLine("Please provide the file path as a command line argument.");
            return;
        }

        string filePath = args[0];
        var parser = new TSql150Parser(false);

        using var reader = new StreamReader(filePath);
        var tree = (TSqlScript)parser.Parse(reader, out IList<ParseError> errors);

        // Get top-level statements
        foreach (var batch in tree.Batches)
        {
            foreach (var topLevelStmt in batch.Statements)
            {
                string? name;
                if (topLevelStmt is CreateTriggerStatement createTrigger)
                {
                    name = StatementVisitor.GetSchemaObjectNameString(createTrigger.Name);
                    var triggerObject = StatementVisitor.GetSchemaObjectNameString(createTrigger.TriggerObject.Name);
                    StatementVisitor.WriteDotRelationship(triggerObject, name, "TRIG");
                }
                else if (topLevelStmt is CreateProcedureStatement createProc)
                {
                    name = string.Join(".", StatementVisitor.GetSchemaObjectNameString(createProc.ProcedureReference.Name));
                }
                else
                {
                    continue;
                }

                // Visit all statements within this top-level statement
                var visitor = new StatementVisitor(name);
                topLevelStmt.Accept(visitor);
            }
        }
    }
}

class StatementVisitor(string topLevelObject) : TSqlFragmentVisitor
{
    public override void Visit(TSqlStatement statement)
    {
        //Console.WriteLine($"  {statement.GetType().Name}");
        base.Visit(statement);
    }

    public override void Visit(InsertStatement statement)
    {
        if (statement.InsertSpecification.Target is NamedTableReference table)
        {
            WriteDotRelationship(topLevelObject, GetSchemaObjectNameString(table.SchemaObject), "INSERT");
        }
    }

    public override void Visit(UpdateStatement statement)
        => ProcessUpdateDeleteSpec(statement.UpdateSpecification, "UPDATE");

    public override void Visit(DeleteStatement statement)
        => ProcessUpdateDeleteSpec(statement.DeleteSpecification, "DELETE");

    // Update and Delete statements need special handling because the target object can be an alias
    public void ProcessUpdateDeleteSpec(UpdateDeleteSpecificationBase spec, string label)
    {
        if (spec.Target is NamedTableReference table)
        {
            var targetAlias = table.Alias?.Value ?? string.Join(".", table.SchemaObject.Identifiers.Select(i => i.Value));
            var fromClause = spec.FromClause;
            string? name = null;
            if (fromClause != null)
            {
                var visitor = new TableReferenceVisitor(targetAlias);
                fromClause.Accept(visitor);

                if (visitor.FoundObjectType == typeof(VariableTableReference)) { return; }

                name = visitor.FoundObjectName;
            }
            name ??= GetSchemaObjectNameString(table.SchemaObject);

            WriteDotRelationship(topLevelObject, name, label);
        }
    }

    public override void Visit(OutputIntoClause statement)
    {
        if (statement.IntoTable is NamedTableReference table)
        {
            WriteDotRelationship(topLevelObject, GetSchemaObjectNameString(table.SchemaObject), "INSERT");
        }
    }

    public override void Visit(MergeStatement statement)
    {
        foreach (var actionClause in statement.MergeSpecification.ActionClauses)
        {
            if (statement.MergeSpecification.Target is NamedTableReference targetTable)
            {
                if (actionClause.Action is DeleteMergeAction)
                {
                    WriteDotRelationship(topLevelObject, GetSchemaObjectNameString(targetTable.SchemaObject), "DELETE");
                }
                else if (actionClause.Action is UpdateMergeAction)
                {
                    WriteDotRelationship(topLevelObject, GetSchemaObjectNameString(targetTable.SchemaObject), "UPDATE");
                }
                else if (actionClause.Action is InsertMergeAction)
                {
                    WriteDotRelationship(topLevelObject, GetSchemaObjectNameString(targetTable.SchemaObject), "INSERT");
                }
            }
        }
    }

    public override void Visit(TruncateTableStatement statement)
    {
        WriteDotRelationship(topLevelObject, GetSchemaObjectNameString(statement.TableName), "TRUNC");
    }

    public override void Visit(ExecuteStatement statement)
    {
        if (statement.ExecuteSpecification.ExecutableEntity is ExecutableProcedureReference proc)
        {
            WriteDotRelationship(topLevelObject, GetSchemaObjectNameString(proc.ProcedureReference.ProcedureReference.Name), "EXEC");
        }

        if (statement.ExecuteSpecification.ExecutableEntity is ExecutableStringList)
        {
            WriteDotRelationship(topLevelObject, "EXEC()", "EXEC");
        }
    }

    public override void Visit(BeginDialogStatement statement)
    {
        WriteDotRelationship(topLevelObject, $"{statement.InitiatorServiceName.Value}.{statement.ContractName.Value}", "QUEUE");
    }

    // Helper class to visit all table references in the FROM clause
    public class TableReferenceVisitor(string alias) : TSqlFragmentVisitor
    {
        public string? FoundObjectName { get; private set; }
        public Type? FoundObjectType { get; private set; }

        public override void Visit(VariableTableReference node)
        {
            // Check if this table reference matches our target alias
            if (node.Alias?.Value == alias)
            {
                FoundObjectName = node.Variable.Name;
                FoundObjectType = node.GetType();
            }
        }

        public override void Visit(NamedTableReference node)
        {
            // Check if this table reference matches our target alias
            if (node.Alias?.Value == alias)
            {
                FoundObjectName = GetSchemaObjectNameString(node.SchemaObject);
                FoundObjectType = node.GetType();
            }
        }
    }

    public static string GetSchemaObjectNameString(SchemaObjectName name)
    {
        var svr = name.ServerIdentifier?.Value;
        var db = name.DatabaseIdentifier?.Value;
        var sch = name.SchemaIdentifier?.Value ?? "{MISSING}";
        var obj = name.BaseIdentifier.Value;

        return string.Join(".", new[] { svr, db, sch, obj }.Where(i => i is not null));
    }

    public static void WriteDotRelationship(string parent, string child, string label)
        => Console.WriteLine($"\"{parent}\" -> \"{child}\" [label=\"{label}\"]");
}