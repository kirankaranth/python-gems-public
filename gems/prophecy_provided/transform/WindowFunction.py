from typing import List, Any

from prophecy.cb.server.base.ComponentBuilderBase import *
from pyspark.sql import *
from pyspark.sql.functions import *

from prophecy.cb.ui.uispec import *
from prophecy.cb.ui.UISpecUtil import getColumnsToHighlight, computeTargetName, SchemaFields, getColumnsInSchema, \
    getColumnsToHighlight2, validateSColumn, sanitizedColumn, ColumnsUsage, validateExpTable
from prophecy.cb.ui.uispec import *
from prophecy.cb.util.NumberUtils import parseInt
from prophecy.cb.util.StringUtils import isBlank
from prophecy.cb.server.base import WorkflowContext

@dataclass(frozen=True)
class OrderByRule:
    expression: SColumn
    sortType: str


class WindowFunction(ComponentSpec):
    name: str = "WindowFunction"
    category: str = "Transform"
    gemDescription: str = "Define a WindowSpec and apply Window functions on a DataFrame."
    docUrl: str = "https://docs.prophecy.io/low-code-spark/gems/transform/window-function"

    def optimizeCode(self) -> bool:
        return True

    @dataclass(frozen=True)
    class WindowFunctionProperties(ComponentProperties):
        activeTab: str = "windowPartition"
        columnsSelector: List[str] = field(default_factory=list)
        partitionColumns: List[SColumn] = field(default_factory=list)
        orderColumns: List[OrderByRule] = field(default_factory=list)
        expressionColumns: List[SColumnExpression] = field(default_factory=list)
        specifyFrame: bool = False
        frameType: str = "row"
        frameStart: Optional[str] = None
        frameEnd: Optional[str] = None
        userSpecifiedStart: Optional[str] = None
        userSpecifiedEnd: Optional[str] = None

    def onClickFunc(self, portId: str, column: str, state: Component[WindowFunctionProperties]):
        if state.properties.activeTab == 'windowPartition':
            partitionColumns = state.properties.partitionColumns
            partitionColumns.append(SColumn.getSColumn(sanitizedColumn(column)))
            return state.bindProperties(replace(state.properties,
                                                partitionColumns=partitionColumns))
        elif state.properties.activeTab == 'windowOrder':
            orderColumns = state.properties.orderColumns
            orderColumns.append(OrderByRule(SColumn.getSColumn(sanitizedColumn(column)), "asc"))
            return state.bindProperties(replace(state.properties,
                                                orderColumns=orderColumns))
        elif state.properties.activeTab == 'windowUse':
            expressionColumns = state.properties.expressionColumns
            expressionColumns.append(SColumnExpression(column, SColumn("row_number()")))
            return state.bindProperties(replace(state.properties, expressionColumns=expressionColumns))
        else:
            return state

    def allColumnsSelectionFunc(self, portId: str, state: Component[WindowFunctionProperties]):
        columnsInSchema = getColumnsInSchema(portId, state, SchemaFields.TopLevel)
        if state.properties.activeTab == 'windowPartition':
            partitionColsToBeAdded = list(
                map(lambda column: SColumn.getSColumn(sanitizedColumn(column)), columnsInSchema))
            updatedPartitionColumns = state.properties.partitionColumns
            updatedPartitionColumns.extend(partitionColsToBeAdded)
            return state.bindProperties(replace(state.properties,
                                                partitionColumns=updatedPartitionColumns))
        elif state.properties.activeTab == 'windowOrder':
            orderColsToBeAdded = list(
                map(lambda column: OrderByRule(SColumn.getSColumn(sanitizedColumn(column)), "asc"), columnsInSchema))
            updatedOrderCols = state.properties.orderColumns
            updatedOrderCols.extend(orderColsToBeAdded)
            return state.bindProperties(replace(state.properties,
                                                orderColumns=updatedOrderCols))
        elif state.properties.activeTab == 'windowUse':
            expressionsToBeAdded = list(
                map(lambda column: SColumnExpression(column, SColumn("row_number()")), columnsInSchema))
            updatedExpressions = state.properties.expressionColumns
            updatedExpressions.extend(expressionsToBeAdded)
            return state.bindProperties(replace(state.properties,
                                                expressionColumns=updatedExpressions))
        else:
            return state

    def dialog(self) -> Dialog:
        selectBox = SelectBox("") \
            .addOption("Ascending", "asc") \
            .addOption("Descending", "desc")

        columns = [
            Column(
                "Order Columns",
                "expression.expression",
                ExpressionBox(ignoreTitle=True)
                    .bindLanguage("${record.expression.format}")
                    .withSchemaSuggestions()
                    .bindPlaceholders()

            ),
            Column("Sort", "sortType", selectBox, width="25%")
        ]
        orderTable = BasicTable("Test", columns=columns).bindProperty("orderColumns")
        partitionTable = BasicTable("Test", columns=[
            Column(
                "Partition Column",
                "expression",
                ExpressionBox(ignoreTitle=True)
                    .bindLanguage("${record.format}")
                    .withSchemaSuggestions()
                    .bindPlaceholders()
            )
        ]).bindProperty("partitionColumns")
        portSchemaTabs = PortSchemaTabs(
            allowInportRename=True,
            selectedFieldsProperty="columnsSelector",
            singleColumnClickCallback=self.onClickFunc,
            allColumnsSelectionCallback=self.allColumnsSelectionFunc
        ).importSchema()
        rowStartColumn = StackLayout(height="100%") \
            .addElement(SelectBox("Start")
                        .addOption("Unbounded Preceding", "unboundedPreceding")
                        .addOption("Current Row", "currentRow")
                        .addOption("Row Number", "userPreceding")
                        .bindProperty("frameStart")
                        ) \
            .addElement(Condition()
                        .ifEqual(PropExpr("component.properties.frameStart"), StringExpr("userPreceding"))
                        .then(TextBox("Number of rows").bindPlaceholder("-1").bindProperty("userSpecifiedStart"))
                        )
        rowEndColumn = StackLayout(height="100%") \
            .addElement(SelectBox("End")
                        .addOption("Unbounded Following", "unboundedFollowing")
                        .addOption("Current Row", "currentRow")
                        .addOption("Row Number", "userFollowing")
                        .bindProperty("frameEnd")
                        ) \
            .addElement(Condition()
                        .ifEqual(PropExpr("component.properties.frameEnd"), StringExpr("userFollowing"))
                        .then(TextBox("Number of rows").bindPlaceholder("1").bindProperty("userSpecifiedEnd"))
                        )

        rangeStartColumn = StackLayout(height="100%") \
            .addElement(SelectBox("Start")
                        .addOption("Unbounded Preceding", "unboundedPreceding")
                        .addOption("Current Row", "currentRow")
                        .addOption("Range Value", "userPreceding")
                        .bindProperty("frameStart")
                        ) \
            .addElement(Condition()
                        .ifEqual(PropExpr("component.properties.frameStart"), StringExpr("userPreceding"))
                        .then(TextBox("Value Preceding").bindPlaceholder("-1000").bindProperty("userSpecifiedStart"))
                        )

        rangeEndColumn = StackLayout(height="100%") \
            .addElement(SelectBox("End")
                        .addOption("Unbounded Following", "unboundedFollowing")
                        .addOption("Current Row", "currentRow")
                        .addOption("Range Value", "userFollowing")
                        .bindProperty("frameEnd")
                        ) \
            .addElement(Condition()
                        .ifEqual(PropExpr("component.properties.frameEnd"), StringExpr("userFollowing"))
                        .then(TextBox("Value Following").bindPlaceholder("1000").bindProperty("userSpecifiedEnd"))
                        )

        windowPane1 = TabPane("PartitionBy", "windowPartition").addElement(partitionTable)
        windowPane2 = TabPane("OrderBy", "windowOrder").addElement(orderTable)
        windowPane3 = TabPane("Frame", "windowFrame") \
            .addElement(StackLayout()
                        .addElement(Checkbox("Specify Frame").bindProperty("specifyFrame")) \
                        .addElement(NativeText("Unselecting above uses default frame specifications."))
                        .addElement(Condition()
                                    .ifEqual(PropExpr("component.properties.specifyFrame"), BooleanExpr(True))
                                    .then(StackLayout(height="100%")
                                          .addElement(RadioGroup("Frame Type")
                                                      .addOption("Row Frame", "row")
                                                      .addOption("Range Frame", "range")
                                                      .bindProperty("frameType"))
                                          .addElement(TitleElement("Frame Boundaries"))
                                          .addElement(Condition()
                                                      .ifEqual(PropExpr("component.properties.frameType"),
                                                               StringExpr("row"))
                                                      .then(ColumnsLayout(gap="1rem")
                                                            .addColumn(rowStartColumn)
                                                            .addColumn(rowEndColumn))
                                                      .otherwise(ColumnsLayout(gap="1rem")
                                                                 .addColumn(rangeStartColumn)
                                                                 .addColumn(rangeEndColumn))))))
        windowPane4 = TabPane("Window Use", "windowUse") \
            .addElement(StackLayout(height="100%")
                        .addElement(NativeText("The below expressions are already computed over the defined window "
                                               "spec." + "User is not required to provide .over(window) in the "
                                                         "expressions below."))
                        .addElement(ExpTable("Expressions",
                                             targetColumn=Column("Target Column", "target",
                                                                 TextBox("", ignoreTitle=True), width="30%"),
                                             expressionColumn=Column("Source Expression", "expression.expression",
                                                                     ExpressionBox(ignoreTitle=True)
                                                                     .bindLanguage("${record.expression.format}")
                                                                     .bindPlaceholders({
                                                                         "scala": """row_number()""",
                                                                         "python": """row_number()""",
                                                                         "sql": """row_number()"""
                                                                     })
                                                                     .withSchemaSuggestions()))
                                    .bindProperty("expressionColumns")))
        windowTabs = Tabs() \
            .bindProperty("activeTab") \
            .addTabPane(windowPane1) \
            .addTabPane(windowPane2) \
            .addTabPane(windowPane3) \
            .addTabPane(windowPane4)
        return Dialog("WindowFunction") \
            .addElement(
            ColumnsLayout(gap="1rem", height="100%")
                .addColumn(portSchemaTabs, "2fr")
                .addColumn(windowTabs, "5fr")
        )

    def validate(self, context: WorkflowContext, component: Component[WindowFunctionProperties]) -> List[Diagnostic]:
        diagnostics = []
        # validate expressionColumns
        if len(component.properties.expressionColumns) == 0:
            diagnostics.append(
                Diagnostic("properties.expressionColumns", "At least one expression in required. [Window Use]",
                           SeverityLevelEnum.Error))
        else:
            expressionDiagnostics = validateExpTable(component.properties.expressionColumns, "expressionColumns",
                                                     component)
            diagnostics.extend([x.appendMessage("[Window Use]") for x in expressionDiagnostics])

        #  validate orderColumns
        if len(component.properties.orderColumns) > 0:
            for idx, expr in enumerate(component.properties.orderColumns):
                orderColDiagnoistics = validateSColumn(
                    expr.expression,
                    f"orderColumns[{idx}].expression",
                    component,
                    ColumnsUsage.WithAndWithoutInputAlias
                )
                diagnostics.extend([x.appendMessage("[OrderBy]") for x in orderColDiagnoistics])

        # validate partitionColumns
        if len(component.properties.partitionColumns) > 0:
            for idx, expr in enumerate(component.properties.partitionColumns):
                partitionColDiagnoistics = validateSColumn(
                    expr,
                    f"partitionColumns[{idx}]",
                    component,
                    ColumnsUsage.WithAndWithoutInputAlias
                )
                diagnostics.extend([x.appendMessage("[PartitionBy]") for x in partitionColDiagnoistics])

        if component.properties.specifyFrame:
            if component.properties.frameType == "range" and len(component.properties.orderColumns) == 0:
                diagnostics.append(Diagnostic("properties.userSpecifiedStart",
                                              "A range window frame cannot be used in an unordered window specification. [Frame]",
                                              SeverityLevelEnum.Error))
            else:
                if isBlank(component.properties.frameStart):
                    diagnostics.append(
                        Diagnostic("properties.frameStart", "Frame start boundary has to be specified [Frame]",
                                   SeverityLevelEnum.Error))
                elif component.properties.frameStart == "userPreceding":
                    if parseInt(component.properties.userSpecifiedStart) is None:
                        diagnostics.append(
                            Diagnostic("properties.userSpecifiedStart", "Frame start boundary has to be a Long [Frame]",
                                       SeverityLevelEnum.Error))

                if isBlank(component.properties.frameEnd):
                    diagnostics.append(
                        Diagnostic("properties.frameEnd", "Frame end boundary has to be specified [Frame]",
                                   SeverityLevelEnum.Error))
                elif component.properties.frameEnd == "userFollowing":
                    if parseInt(component.properties.userSpecifiedEnd) is None:
                        diagnostics.append(
                            Diagnostic("properties.userSpecifiedEnd", "Frame end boundary has to be a Long [Frame]",
                                       SeverityLevelEnum.Error))
                if not isBlank(component.properties.frameStart) and not isBlank(component.properties.frameEnd):
                    if component.properties.frameStart == "currentRow":
                        frameStartInt = 0
                    elif component.properties.frameStart == "unboundedPreceding":
                        frameStartInt = Window.unboundedPreceding  # -9223372036854775808
                    elif component.properties.frameStart == "userPreceding":
                        if isBlank(component.properties.userSpecifiedStart):
                            frameStartInt = None
                        else:
                            frameStartInt = parseInt(component.properties.userSpecifiedStart)
                    else:
                        frameStartInt = 0

                    if component.properties.frameEnd == "currentRow":
                        frameEndInt = 0
                    elif component.properties.frameEnd == "unboundedFollowing":
                        frameEndInt = Window.unboundedFollowing  # 9223372036854775807
                    elif component.properties.frameEnd == "userFollowing":
                        if isBlank(component.properties.userSpecifiedEnd):
                            frameEndInt = None
                        else:
                            frameEndInt = parseInt(component.properties.userSpecifiedEnd)
                    else:
                        frameEndInt = 0
                    if frameStartInt is not None and frameEndInt is not None and frameStartInt > frameEndInt:
                        diagnostics.append(Diagnostic("properties.userSpecifiedStart",
                                                      "Frame start boundary can not lie after the frame end boundary "
                                                      "[Frame]", SeverityLevelEnum.Error))
        return diagnostics

    def onChange(self, context: WorkflowContext, oldState: Component[WindowFunctionProperties], newState: Component[WindowFunctionProperties]) -> \
            Component[WindowFunctionProperties]:
        newProps = newState.properties
        usedCols = getColumnsToHighlight(
            newProps.partitionColumns + newProps.expressionColumns + [x.expression for x in newProps.orderColumns],
            newState
        )
        updatedprops = newProps
        if newProps.frameType != oldState.properties.frameType:
            updatedprops = replace(updatedprops, frameStart=None, frameEnd=None)
        if newProps.frameStart != oldState.properties.frameStart:
            updatedprops = replace(updatedprops, userSpecifiedStart=None)
        if newProps.frameEnd != oldState.properties.frameEnd:
            updatedprops = replace(updatedprops, userSpecifiedEnd=None)

        return newState.bindProperties(
            replace(updatedprops,
                    columnsSelector=usedCols,
                    activeTab=newProps.activeTab)
        )

    class WindowFunctionCode(ComponentCode):
        def __init__(self, newProps):
            self.props: WindowFunction.WindowFunctionProperties = newProps

        def apply(self, spark: SparkSession, in0: DataFrame) -> DataFrame:
            orderRules = map(lambda x:
                             x.expression.column().asc() if (x.sortType == "asc") else x.expression.column().desc(),
                             self.props.orderColumns)

            if self.props.frameStart == "currentRow":
                fStart = Window.currentRow
            elif self.props.frameStart == "unboundedPreceding":
                fStart = Window.unboundedPreceding
            else:
                fStart = int(self.props.userSpecifiedStart)

            if self.props.frameEnd == "currentRow":
                fEnd = Window.currentRow
            elif self.props.frameEnd == "unboundedFollowing":
                fEnd = Window.unboundedFollowing
            else:
                fEnd = int(self.props.userSpecifiedEnd)

            if len(self.props.partitionColumns) > 0:
                partSpec: WindowSpec = Window.partitionBy(*list(map(lambda x: x.column(), self.props.partitionColumns)))
            else:
                partSpec: WindowSpec = Window.partitionBy()

            if len(self.props.orderColumns) > 0:
                orderSpec: WindowSpec = partSpec.orderBy(*orderRules)
            else:
                orderSpec: WindowSpec = partSpec

            if self.props.specifyFrame:
                if self.props.frameType == "range":
                    windowWithFrame: WindowSpec = orderSpec.rangeBetween(fStart, fEnd)
                elif self.props.frameType == "row":
                    windowWithFrame: WindowSpec = orderSpec.rowsBetween(fStart, fEnd)
                else:
                    windowWithFrame: WindowSpec = orderSpec
            else:
                windowWithFrame: WindowSpec = orderSpec

            out = in0
            for expression in self.props.expressionColumns:
                out = out.withColumn(expression.target, expression.expression.column().over(windowWithFrame))

            return out
