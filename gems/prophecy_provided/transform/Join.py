from pyspark.sql import *

from prophecy.cb.server.base.ComponentBuilderBase import *
from prophecy.cb.ui.UISpecUtil import getColumnsToHighlight2, computeTargetName, getColumnsInSchema, SchemaFields, \
    getColumnsToHighlight, ColumnsUsage, validateSColumn, validateExpTable, sanitizedColumn
from prophecy.cb.ui.uispec import *
from prophecy.cb.server.base import WorkflowContext

@dataclass(frozen=True)
class Hint:
    id: str
    alias: str
    hintType: str
    propagateColumns: bool


@dataclass(frozen=True)
class JoinCondition:
    alias: str
    expression: SColumn
    joinType: str


class Join(ComponentSpec):
    name: str = "Join"
    category: str = "Join/Split"
    gemDescription: str = "Join multiple dataframes"
    docUrl: str = "https://docs.prophecy.io/low-code-spark/gems/join-split/join/"

    def optimizeCode(self) -> bool:
        return True

    @dataclass(frozen=True)
    class JoinProperties(ComponentProperties):
        activeTab: str = "conditions"
        columnsSelector: List[str] = field(default_factory=list)
        conditions: List[JoinCondition] = field(default_factory=lambda: [JoinCondition("in1", SColumn(""), "inner")])
        expressions: List[SColumnExpression] = field(default_factory=list)
        headAlias: str = "in0"
        whereClause: Optional[SColumn] = None
        allIn0: Optional[bool] = None
        allIn1: Optional[bool] = None
        hints: Optional[List[Hint]] = field(default_factory=lambda: [Hint("in0", "in0", "none", False),
                                                                     Hint("in1", "in1", "none", False)])

    def onClickFunc(self, portId: str, column: str, state: Component[JoinProperties]):
        if state.properties.activeTab == "expressions":
            portSlug: str = [x.slug for x in state.ports.inputs if x.id == portId][0]
            existingTargetNames = list(map(lambda exp: exp.target, state.properties.expressions))
            targetColOptions = list(column.split('.'))
            targetCol = computeTargetName(targetColOptions[-1], targetColOptions[:-1], existingTargetNames)
            expressions = state.properties.expressions
            expressions.append(SColumnExpression(targetCol,
                                                 SColumn.getSColumn(f"{portSlug}.{sanitizedColumn(column)}"), ""))
            return state.bindProperties(replace(state.properties, expressions=expressions))
        else:
            return state

    def allColumnsSelectionFunc(self, portId: str, state: Component[JoinProperties]):
        if state.properties.activeTab == "expressions":
            portSlug: str = [x.slug for x in state.ports.inputs if x.id == portId][0]
            columnsInSchema = getColumnsInSchema(portId, state, SchemaFields.TopLevel)
            expressionsToAdd = list(
                map(lambda column: SColumnExpression(column,
                                                     SColumn.getSColumn(f"{portSlug}.{sanitizedColumn(column)}"), ""),
                    columnsInSchema))
            updatedExpressions = state.properties.expressions
            updatedExpressions.extend(expressionsToAdd)
            return state.bindProperties(replace(state.properties, expressions=updatedExpressions))
        else:
            return state

    def dialog(self) -> Dialog:
        return Dialog("Join").addElement(
            ColumnsLayout(gap=("1rem"), height=("100%"))
                .addColumn(
                PortSchemaTabs(
                    editableInput=(True),
                    minNumberOfPorts=2,
                    selectedFieldsProperty=("columnsSelector"),
                    singleColumnClickCallback=self.onClickFunc,
                    allColumnsSelectionCallback=self.allColumnsSelectionFunc).importSchema()
            )
                .addColumn(
                Tabs()
                    .bindProperty("activeTab")
                    .addTabPane(
                    TabPane("Conditions", "conditions")
                        .addElement(
                        StackLayout(height=("100%"))
                            .addElement(BasicTable(
                            "Test",
                            columns=[
                                Column("Input Alias", "alias", width="25%"),
                                Column(
                                    "Join Condition",
                                    "expression.expression",
                                    (
                                        ExpressionBox(ignoreTitle=True)
                                            .bindLanguage("${record.expression.format}")
                                            .withSchemaSuggestions()
                                            .bindPlaceholders({
                                            "scala": """col("in0.source_column") === col("in1.other_column")""",
                                            "python": """col("in0.source_column") == col("in1.other_column")""",
                                            "sql": """in0.source_column = in1.other_column"""
                                        })
                                    )
                                ),
                                Column("Type", "joinType",
                                       (SelectBox("")
                                        .addOption("Inner", "inner")
                                        .addOption("Outer", "outer")
                                        .addOption("Left Outer", "left_outer")
                                        .addOption("Right Outer", "right_outer")
                                        .addOption("Left Semi", "left_semi")
                                        .addOption("Left Anti", "left_anti")
                                        ), width="20%")
                            ],
                            targetColumnKey="alias",
                            delete=False,
                            appendNewRow=False
                        ).bindProperty("conditions")
                                        )
                            .addElement(TitleElement("Where Clause"))
                            .addElement(
                            Editor(height=("20%"))
                                .makeFieldOptional()
                                .withSchemaSuggestions()
                                .bindProperty("whereClause")
                        )
                    )
                )
                    .addTabPane(
                    TabPane("Expressions", "expressions")
                        .addElement(
                        ExpTable("Expressions")
                            .bindProperty("expressions")
                    )
                )
                    .addTabPane(
                    TabPane("Advanced", "advanced")
                        .addElement(
                        BasicTable(
                            "Test",
                            columns=[
                                Column("Input Alias", "alias", width="25%"),
                                Column("Hint Type", "hintType",
                                       (SelectBox("")
                                        .addOption("None", "none")
                                        .addOption("Broadcast", "broadcast")
                                        .addOption("Merge", "merge")
                                        .addOption("Shuffle Hash", "shuffle_hash")
                                        .addOption("Shuffle Replicate NL", "shuffle_replicate_nl")
                                        ), width="80%"),
                                Column("Propagate All Columns", "propagateColumns",
                                       (Checkbox("").bindProperty("record.propagateColumns")), width="80%")
                            ],
                            delete=False,
                            appendNewRow=False,
                            targetColumnKey="alias",
                        ).bindProperty("hints")
                    )
                ),
                "2fr"
            )
        )

    def validate(self, context: WorkflowContext, component: Component[JoinProperties]) -> List[Diagnostic]:
        diagnostics = []

        if (len(component.ports.inputs) < 2):
            diagnostics.append(Diagnostic(
                "ports.inputs",
                "Input ports can't be less than two in Join transformation",
                SeverityLevelEnum.Error
            ))
        else:
            if (len(component.properties.conditions) > 0):
                for idx, expr in enumerate(component.properties.conditions):
                    _d1 = validateSColumn(
                        expr.expression,
                        f"conditions[{idx}].expression",
                        component,
                        ColumnsUsage.WithAndWithoutInputAlias
                    )
                    d1 = [x.appendMessage("[Conditions]") for x in _d1]
                    diagnostics.extend(d1)

            if (
                    component.properties.whereClause is not None and not component.properties.whereClause.isExpressionPresent()):
                diagnostics.append(Diagnostic(
                    "properties.whereClause.expression",
                    f"""Unsupported expression {component.properties.whereClause.rawExpression} [Conditions]""",
                    SeverityLevelEnum.Error
                ))

            _d2 = validateExpTable(
                component.properties.expressions,
                "expressions",
                component,
                (ColumnsUsage.WithAndWithoutInputAlias)
            )
            d2 = [x.appendMessage("[Expressions]") for x in _d2]
            diagnostics.extend(d2)

        return diagnostics

    def onChange(self, context: WorkflowContext, oldState: Component[JoinProperties], newState: Component[JoinProperties]) -> Component[
        JoinProperties]:
        newProps = newState.properties
        oldConditions: list[JoinCondition] = oldState.properties.conditions

        conditionsAfterChangeApplication: list[JoinCondition] = [replace(x) for x in oldConditions]
        slugChanged = [(oldPort, idx) for idx, oldPort in enumerate(oldState.ports.inputs) for newPort in
                       newState.ports.inputs if ((oldPort.id == newPort.id) and (oldPort.slug != newPort.slug))]

        for (port, idx) in slugChanged:
            if idx - 1 >= 0:
                newAlias = next(newPort.slug for newPort in newState.ports.inputs if newPort.id == port.id)
                conditionsAfterChangeApplication[idx - 1] = replace(conditionsAfterChangeApplication[idx - 1],
                                                                    alias=newAlias)

        newStateInPortIDs = [x.id for x in newState.ports.inputs]
        deletedPorts = [(oldPort, idx) for idx, oldPort in enumerate(oldState.ports.inputs) if
                        oldPort.id not in newStateInPortIDs]

        conditionIndicesToDelete = list(set([x[1] - 1 if ((x[1] - 1) >= 0) else 0 for x in deletedPorts]))
        conditionIndicesToDelete.sort(reverse=True)

        for idx in conditionIndicesToDelete:
            conditionsAfterChangeApplication = conditionsAfterChangeApplication[
                                               :idx] + conditionsAfterChangeApplication[idx + 1:]

        oldStateInPortIDs = [x.id for x in oldState.ports.inputs]
        addedPorts = [newPort for newPort in newState.ports.inputs if newPort.id not in oldStateInPortIDs]

        hintsAfterChangeApplication = []

        for newPort in newState.ports.inputs:
            flag = True
            for hint in newState.properties.hints:
                if newPort.id == hint.id:
                    if newPort.slug == "in0" and newProps.allIn0:
                        hintsAfterChangeApplication.append(
                            Hint(newPort.id, newPort.slug, hint.hintType, True))
                    elif newPort.slug == "in1" and newProps.allIn1:
                        hintsAfterChangeApplication.append(
                            Hint(newPort.id, newPort.slug, hint.hintType, True))
                    else:
                        hintsAfterChangeApplication.append(
                            Hint(newPort.id, newPort.slug, hint.hintType, hint.propagateColumns))
                    flag = False
                    break
            if flag:
                if newPort.slug == "in0" and newProps.allIn0:
                    hintsAfterChangeApplication.append(Hint(newPort.id, newPort.slug, "none", True))
                elif newPort.slug == "in1" and newProps.allIn1:
                    hintsAfterChangeApplication.append(Hint(newPort.id, newPort.slug, "none", True))
                else:
                    hintsAfterChangeApplication.append(Hint(newPort.id, newPort.slug, "none", False))

        conditionsToAdd = []
        lastSlug = newState.ports.inputs[-2].slug
        for addedPort in addedPorts:
            conditionsToAdd += [JoinCondition(addedPort.slug, SColumn(""), "inner")]
            lastSlug = addedPort.slug

        conditionsAfterChangeApplication.extend(conditionsToAdd)

        usedCols = getColumnsToHighlight2(
            [e.expression for e in newProps.conditions],
            newState,
            ColumnsUsage.WithAndWithoutInputAlias
        ) + getColumnsToHighlight(
            newProps.expressions,
            newState,
            ColumnsUsage.WithAndWithoutInputAlias
        )

        if conditionsAfterChangeApplication == oldConditions:
            finalConditions = newState.properties.conditions
        else:
            finalConditions = conditionsAfterChangeApplication

        print("old conditions ", oldConditions)
        print("conditionsAfterChangeApplication ", conditionsAfterChangeApplication)
        print("finalConditions ", finalConditions)

        if newProps.whereClause is not None and newProps.whereClause.isExpressionPresent():
            updatedWhereClause = newProps.whereClause
        else:
            updatedWhereClause = None

        if newProps.allIn0 or newProps.allIn1:
            return newState.bindProperties(
                replace(newProps,
                        columnsSelector=usedCols,
                        expressions=newProps.expressions,
                        conditions=finalConditions,
                        activeTab=newProps.activeTab,
                        headAlias=newState.ports.inputs[0].slug,
                        whereClause=updatedWhereClause,
                        hints=hintsAfterChangeApplication,
                        allIn0=False,
                        allIn1=False))
        else:
            return newState.bindProperties(
                replace(newProps,
                        columnsSelector=usedCols,
                        expressions=newProps.expressions,
                        conditions=finalConditions,
                        activeTab=newProps.activeTab,
                        headAlias=newState.ports.inputs[0].slug,
                        whereClause=updatedWhereClause,
                        hints=hintsAfterChangeApplication))

    class JoinCode(ComponentCode):
        def __init__(self, newProps):
            self.props: Join.JoinProperties = newProps

        def apply(self, spark: SparkSession, in0: DataFrame, in1: DataFrame, *inDFs: DataFrame) -> DataFrame:

            df_in0 = in0.alias(self.props.headAlias)

            if self.props.hints is not None and self.props.hints[0].hintType != "none":
                res = df_in0.hint(self.props.hints[0].hintType)
            else:
                res = df_in0

            propagate_cols = []
            if self.props.hints[0].propagateColumns:
                propagate_cols.append(col(self.props.hints[0].alias + ".*"))
            elif self.props.allIn0:
                propagate_cols.append(col("in0.*"))

            _inputs = [in1]
            _inputs.extend(inDFs)
            inputConditionPair = list(zip(_inputs, self.props.conditions))
            inputConditionPairWithHints = list(zip(inputConditionPair, self.props.hints[1:]))

            for pair in inputConditionPairWithHints:
                _pairCondition, _hint = pair
                inPort, _condition = _pairCondition
                if _hint.hintType == "none":
                    nextDF = inPort.alias(_condition.alias)
                else:
                    nextDF = inPort.hint(_hint.hintType).alias(_condition.alias)
                res = res.join(nextDF, _condition.expression.column(), _condition.joinType)
                if _hint.propagateColumns:
                    propagate_cols.append(col(_hint.alias + ".*"))
                if _condition.alias == "in1" and self.props.allIn1:
                    propagate_cols.append(col("in1.*"))

            if self.props.whereClause is None:
                resFiltered = res
            else:
                resFiltered = res.where(self.props.whereClause.column())

            if (  #skipEagerEvaluation
                    len(self.props.expressions) > 0):
                if (  #skipEagerEvaluation
                        len(propagate_cols) > 0):
                    return resFiltered.select(*list(map(lambda x: x.column(), self.props.expressions)), *propagate_cols)
                else:
                    return resFiltered.select(*list(map(lambda x: x.column(), self.props.expressions)))
            else:
                return resFiltered
