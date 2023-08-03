from prophecy.cb.server.base.ComponentBuilderBase import *
from pyspark.sql import *
from pyspark.sql.functions import *

from prophecy.cb.ui.UISpecUtil import getColumnsToHighlight, computeTargetName, SchemaFields, getColumnsInSchema, \
    validateExpTable, ColumnsUsage, getTargetTokens, sanitizedColumn
from prophecy.cb.ui.uispec import *
from prophecy.cb.util.StringUtils import isBlank


class Reformat(ComponentSpec):
    name: str = "Reformat"
    category: str = "Transform"

    def optimizeCode(self) -> bool:
        return True

    @dataclass(frozen=True)
    class ReformatProperties(ComponentProperties):
        columnsSelector: List[str] = field(default_factory=list)
        expressions: List[SColumnExpression] = field(default_factory=list)

    def onClickFunc(self, portId: str, column: str, state: Component[ReformatProperties]):
        existingTargetNames = list(map(lambda exp: exp.target, state.properties.expressions))
        targetTokens = getTargetTokens(column, [exp.split('.') for exp in existingTargetNames], True)
        targetCol = '.'.join(targetTokens)
        expressions = state.properties.expressions
        expressions.append(SColumnExpression(targetCol, SColumn.getSColumn(sanitizedColumn(column)), ""))
        return state.bindProperties(replace(state.properties, expressions=expressions))

    def allColumnsSelectionFunc(self, portId: str, state: Component[ReformatProperties]):
        columnsInSchema = getColumnsInSchema(portId, state, SchemaFields.TopLevel)
        expressions = list(
            map(lambda column: SColumnExpression.getSColumnExpression(sanitizedColumn(column)), columnsInSchema))
        state.properties.expressions.extend(expressions)
        return state.bindProperties(replace(state.properties, expressions=state.properties.expressions))

    def dialog(self) -> Dialog:
        return Dialog("Reformat").addElement(
            ColumnsLayout(gap="1rem", height="100%")
            .addColumn(
                PortSchemaTabs(
                    allowInportRename=True,
                    selectedFieldsProperty="columnsSelector",
                    singleColumnClickCallback=self.onClickFunc,
                    allColumnsSelectionCallback=self.allColumnsSelectionFunc
                ).importSchema(),
                "2fr"
            )
            .addColumn(
                ExpTable("Reformat Expression")
                .enableVirtualization()
                .bindProperty("expressions"),
                "5fr"
            )
        )

    def get_me_diags(self):
        return Diagnostic(path='path', message='new message in demo', severity=SeverityLevelEnum.Error)

    def validate(self, component: Component[ReformatProperties]) -> List[Diagnostic]:
        diagnostics = []
        expTableDiags = validateExpTable(component.properties.expressions, "expressions", component,
                                         ColumnsUsage.WithoutInputAlias)
        diagnostics.extend(expTableDiags)
        diagnostics.extend(self.get_me_diags())
        return diagnostics

    def onChange(self, oldState: Component[ReformatProperties], newState: Component[ReformatProperties]) -> Component[
        ReformatProperties]:
        newProps = newState.properties
        usedColExps = getColumnsToHighlight(newProps.expressions, newState)
        return newState.bindProperties(replace(newProps,
                                               columnsSelector=usedColExps,
                                               expressions=list(
                                                   map(lambda exp: exp.withRowId(), newProps.expressions))))

    class ReformatCode(ComponentCode):
        def __init__(self, newProps):
            self.props: Reformat.ReformatProperties = newProps

        def apply(self, spark: SparkSession, in0: DataFrame) -> DataFrame:
            if (  # skipEagerEvaluation
                    len(self.props.expressions) > 0):
                selectColumns = map(lambda x: x.column(), self.props.expressions)
                return in0.select(*selectColumns)
            else:
                return in0
