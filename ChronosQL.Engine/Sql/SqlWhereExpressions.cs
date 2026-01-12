namespace ChronosQL.Engine.Sql;

public abstract record SqlCondition;

public sealed record SqlBinaryCondition(SqlCondition Left, SqlBinaryOperator Operator, SqlCondition Right) : SqlCondition;

public sealed record SqlNotCondition(SqlCondition Condition) : SqlCondition;

public sealed record SqlPredicateCondition(SqlPredicate Predicate) : SqlCondition;

public enum SqlBinaryOperator
{
    And,
    Or
}

public abstract record SqlPredicate;

public sealed record SqlComparisonPredicate(SqlExpression Left, SqlComparisonOperator Operator, SqlExpression Right) : SqlPredicate;

public sealed record SqlLikePredicate(SqlExpression Expression, SqlExpression Pattern, bool Negated) : SqlPredicate;

public sealed record SqlBetweenPredicate(SqlExpression Expression, SqlExpression Lower, SqlExpression Upper, bool Negated) : SqlPredicate;

public sealed record SqlIsNullPredicate(SqlExpression Expression, bool Negated) : SqlPredicate;

public sealed record SqlInPredicate(SqlExpression Expression, IReadOnlyList<SqlExpression> Values, bool Negated) : SqlPredicate;

public enum SqlComparisonOperator
{
    Equal,
    NotEqual,
    GreaterThan,
    GreaterThanOrEqual,
    LessThan,
    LessThanOrEqual,
    NotGreaterThan,
    NotLessThan
}

public abstract record SqlExpression;

public sealed record SqlFieldExpression(FieldReference Field) : SqlExpression;

public sealed record SqlLiteralExpression(SqlLiteral Literal) : SqlExpression;

public sealed record SqlLiteral(SqlLiteralKind Kind, string? Text, double? Number, bool? Boolean);

public enum SqlLiteralKind
{
    Null,
    String,
    Number,
    Boolean
}
