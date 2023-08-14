from pyspark.sql import *
from pyspark.sql.functions import *

from prophecy.cb.server.base.ComponentBuilderBase import *
from prophecy.cb.ui.UISpecUtil import (
    getColumnsToHighlight,
    SchemaFields,
    getColumnsInSchema,
    validateExpTable,
    ColumnsUsage,
    validateSColumn,
    sanitizedColumn,
)
from prophecy.cb.ui.uispec import *
from prophecy.cb.server.base import WorkflowContext

@dataclass(frozen=True)
class StringColName:
    colName: str


class Aggregate(ComponentSpec):
    name: str = "Aggregate"
    category: str = "Transform"
    gemDescription: str = "Performs a Group-by and Aggregate Operation"
    docUrl: str = "https://docs.prophecy.io/low-code-spark/gems/transform/aggregate/"

    def optimizeCode(self) -> bool:
        return True

    @dataclass(frozen=True)
    class AggregateProperties(ComponentProperties):
        activeTab: str = "aggregate"
        columnsSelector: List[str] = field(default_factory=list)
        groupBy: List[SColumnExpression] = field(default_factory=list)
        aggregate: List[SColumnExpression] = field(default_factory=list)
        doPivot: bool = False
        pivotColumn: Optional[SColumn] = None
        pivotValues: List[StringColName] = field(default_factory=list)
        allowSelection: Optional[bool] = True
        allIns: Optional[bool] = False

    def onClickFunc(
            self, portId: str, column: str, state: Component[AggregateProperties]
    ):
        if state.properties.activeTab == "groupBy":
            groupbys = state.properties.groupBy
            groupbys.append(
                SColumnExpression.getSColumnExpression(sanitizedColumn(column))
            )
            return state.bindProperties(
                replace(state.properties, groupBy=groupbys, allowSelection=True)
            )
        elif state.properties.activeTab == "aggregate":
            aggregates = state.properties.aggregate
            aggregates.append(
                SColumnExpression(
                    column,
                    SColumn(
                        f"""first(col("{sanitizedColumn(column)}"))""",
                        "python",
                        first(col(f"{sanitizedColumn(column)}")),
                        [column],
                    ),
                )
            )
            return state.bindProperties(
                replace(state.properties, aggregate=aggregates, allowSelection=True)
            )
        else:
            return state.bindProperties(replace(state.properties, allowSelection=False))

    def allColumnsSelectionFunc(
            self, portId: str, state: Component[AggregateProperties]
    ):
        columnsInSchema = getColumnsInSchema(portId, state, SchemaFields.TopLevel)
        res = state
        for c in columnsInSchema:
            res = self.onClickFunc(portId, c, state)
        return res

    def dialog(self) -> Dialog:
        return Dialog("Aggregate").addElement(
            ColumnsLayout(gap="1rem", height="100%")
                .addColumn(
                PortSchemaTabs(
                    selectedFieldsProperty="columnsSelector",
                    singleColumnClickCallback=self.onClickFunc,
                    allColumnsSelectionCallback=self.allColumnsSelectionFunc,
                )
                    .allowColumnClickBasedOn("allowSelection")
                    .importSchema(),
                "2fr",
            )
                .addColumn(
                Tabs()
                    .bindProperty("activeTab")
                    .addTabPane(
                    TabPane("Aggregate", "aggregate").addElement(
                        StackLayout(height="100%")
                            .addElement(
                            ExpTable("Aggregate Expressions").bindProperty("aggregate")
                        )
                            .addElement(
                            ColumnsLayout().addColumn(
                                Checkbox("Propagate all input columns").bindProperty(
                                    "allIns"
                                )
                            )
                        )
                    )
                )
                    .addTabPane(
                    TabPane("Group By", "groupBy").addElement(
                        ExpTable("Group By Columns").bindProperty("groupBy")
                    )
                )
                    .addTabPane(
                    TabPane("Pivot", "pivot")
                        .addElement(Checkbox("Do pivot").bindProperty("doPivot"))
                        .addElement(
                        Condition()
                            .ifEqual(
                            PropExpr("component.properties.doPivot"), BooleanExpr(True)
                        )
                            .then(
                            StackLayout(gap="1rem")
                                .addElement(
                                ExpressionBox("Pivot Column")
                                    .makeFieldOptional()
                                    .withSchemaSuggestions()
                                    .bindPlaceholders(
                                    {
                                        "scala": """col("col_name")""",
                                        "python": """col("col_name")""",
                                        "sql": """col_name""",
                                    }
                                )
                                    .bindProperty("pivotColumn")
                            )
                                .addElement(
                                BasicTable(
                                    "Unique Values",
                                    height="400px",
                                    columns=[
                                        Column(
                                            "Unique Values",
                                            "colName",
                                            (
                                                TextBox("").bindPlaceholder(
                                                    "Enter value present in pivot column"
                                                )
                                            ),
                                        )
                                    ],
                                ).bindProperty("pivotValues")
                            )
                        )
                    )
                ),
                "5fr",
            )
        )

    def validate(self, context: WorkflowContext, component: Component[AggregateProperties]) -> List[Diagnostic]:
        diagnostics = []
        if len(component.properties.aggregate) == 0:
            diagnostics.append(
                Diagnostic(
                    "properties.aggregate",
                    "At least one aggregate expression is required in Aggregate.",
                    SeverityLevelEnum.Error,
                )
            )
        else:
            # validate aggregate tab
            aggregateTabDiagnostics = validateExpTable(
                component.properties.aggregate, "aggregate", component
            )
            diagnostics += [
                x.appendMessage("[Aggregate]") for x in aggregateTabDiagnostics
            ]

            # validate groupby tab
            _d2 = validateExpTable(component.properties.groupBy, "groupBy", component)
            d2 = map(lambda x: x.appendMessage("[GroupBy]"), _d2)
            diagnostics.extend(d2)

            # validate pivot tab
            if component.properties.doPivot:
                if len(component.properties.groupBy) == 0:
                    diagnostics.append(
                        Diagnostic(
                            "properties.pivotColumn.expression",
                            "Pivot operation is only supported with groupBy operation. Please fill groupBy columns.",
                            SeverityLevelEnum.Error,
                        )
                    )

                elif component.properties.pivotColumn is not None:
                    d3 = [
                        d.appendMessage("[Pivot]")
                        for d in validateSColumn(
                            component.properties.pivotColumn,
                            "pivotColumn",
                            component,
                            testColumnPresence=ColumnsUsage.WithoutInputAlias,
                        )
                    ]

                    diagnostics.extend(d3)

                    if len(component.properties.pivotValues) > 0:
                        for idx, pivotVal in enumerate(
                                component.properties.pivotValues
                        ):
                            if len(pivotVal.colName.strip()) == 0:
                                diagnostics.append(
                                    Diagnostic(
                                        f"properties.pivotValues[{idx}].colName",
                                        "Row cannot be empty. [Pivot]",
                                        SeverityLevelEnum.Error,
                                    )
                                )
                else:
                    diagnostics.append(
                        Diagnostic(
                            "properties.pivotColumn.expression",
                            "Pivot column cannot be empty. [Pivot]",
                            SeverityLevelEnum.Error,
                        )
                    )
        return diagnostics

    def onChange(
            self,
            context: WorkflowContext,
            oldState: Component[AggregateProperties],
            newState: Component[AggregateProperties],
    ) -> Component[AggregateProperties]:
        newProps = newState.properties
        used = getColumnsToHighlight(newProps.groupBy + newProps.aggregate, newState)
        if not newProps.doPivot:
            updatedPivotCol = None
        else:
            updatedPivotCol = newProps.pivotColumn

        return newState.bindProperties(
            replace(
                newProps,
                columnsSelector=used,
                activeTab=newProps.activeTab,
                pivotColumn=updatedPivotCol,
                allowSelection=newProps.activeTab != "pivot",
            )
        )

    class AggregateCode(ComponentCode):
        def __init__(self, newProps):
            self.props: Aggregate.AggregateProperties = newProps

        def apply(self, spark: SparkSession, in0: DataFrame) -> DataFrame:
            additional_passthrough = []
            if self.props.allIns:
                agg_cols = [e.target for e in self.props.aggregate]
                groupBy_cols = [e.target for e in self.props.groupBy]
                pivot_cols = (
                    [self.props.pivotColumn.columnName()] if self.props.doPivot else []
                )
                additional_passthrough = [
                    first(col(x)).alias(x)
                    for x in in0.columns
                    if x not in (agg_cols + groupBy_cols + pivot_cols)
                ]

            if len(self.props.groupBy) == 0:
                foo = map(lambda x: x.column(), self.props.aggregate)
                out = in0.agg(*foo, *additional_passthrough)

            else:
                foo1 = map(lambda x: x.column(), self.props.groupBy)
                grouped = in0.groupBy(*foo1)
                if self.props.doPivot:
                    if self.props.pivotColumn is not None:
                        if len(self.props.pivotValues) == 0:
                            pivoted = grouped.pivot(self.props.pivotColumn.columnName())
                        else:  # (len(self.props.pivotValues)>0)
                            pivoted = grouped.pivot(
                                self.props.pivotColumn.columnName(),
                                [x.colName for x in self.props.pivotValues]
                            )
                    else:
                        pivoted = grouped
                else:
                    pivoted = grouped

                foo2 = map(lambda x: x.column(), self.props.aggregate)
                out = pivoted.agg(*foo2, *additional_passthrough)

            return out
