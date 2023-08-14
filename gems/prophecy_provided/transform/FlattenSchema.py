from pyspark.sql import *
from pyspark.sql.functions import *
from prophecy.cb.server.base.ComponentBuilderBase import *
from prophecy.cb.ui.UISpecUtil import *
from prophecy.cb.ui.uispec import *
from pyspark.sql.types import StructType, ArrayType
from typing import Optional, List
from dataclasses import dataclass, field
from prophecy.cb.server.base import WorkflowContext

@dataclass(frozen=True)
class FlattenSchemaExpression:
    target: str
    expression: SColumn
    flattenedExpression: str
    exploded: str
    targetTokens: List[str]


@dataclass(frozen=True)
class ExplodeColInfo:
    originalColToExplode: str
    updatedColToExplode: str
    colNameAfterExplode: str


class FlattenSchema(ComponentSpec):
    name: str = "FlattenSchema"
    category: str = "Transform"
    gemDescription: str = "Flatten Complex nested datatypes into rows and columns"
    docUrl: str = "https://docs.prophecy.io/low-code-spark/gems/transform/flatten-schema"

    def optimizeCode(self) -> bool:
        return True

    @dataclass(frozen=True)
    class FlattenSchemaProperties(ComponentProperties):
        columnsSelector: List[str] = field(default_factory=list)
        delimiter: str = "-"
        fsExpressions: List[FlattenSchemaExpression] = field(default_factory=list)
        explodeColumns: List[str] = field(default_factory=list)
        explodedColsNewName: List[ExplodeColInfo] = field(default_factory=list)

    def getColumnsToExplode(self, traversedColumns: List[str],
                            remainingColumns: List[str], schemaOption: Optional[StructType] = None) -> List[str]:
        if schemaOption is None:
            return []
        else:
            if len(remainingColumns) == 0:
                return []
            else:
                res: List[str] = []
                curr = remainingColumns[0]
                colNameMatchesInSchema = []
                for schemaField in schemaOption.fields:
                    if schemaField.name.lower() == curr.lower():
                        colNameMatchesInSchema.append(schemaField)
                fullPathTillCurr: List[str] = traversedColumns
                fullPathTillCurr.append(curr)

        if len(colNameMatchesInSchema) == 0:
            return []
        else:
            subSchema = colNameMatchesInSchema[0]
            if isinstance(subSchema.dataType, StructType):
                return self.getColumnsToExplode(fullPathTillCurr, remainingColumns[1:], subSchema.dataType)
            elif isinstance(subSchema.dataType, ArrayType):
                if isinstance(subSchema.dataType.elementType, StructType):
                    res.append('.'.join(fullPathTillCurr))
                    res.extend(self.getColumnsToExplode(fullPathTillCurr, remainingColumns[1:],
                                                        subSchema.dataType.elementType))
                    return res
                else:
                    res.append('.'.join(fullPathTillCurr))
                    return res
            else:
                return res

    def onClickFunc(self, portId: str, column: str, state: Component[FlattenSchemaProperties]):

        tokensOfUsedTargetNames = list(map(lambda x: x.targetTokens, state.properties.fsExpressions))
        targetColTokens = getTargetTokens(column, tokensOfUsedTargetNames)
        colsToExplode = self.getColumnsToExplode([], list(column.split('.')),
                                                 list(map(lambda x: x.schema, state.ports.inputs))[0])
        if len(colsToExplode) != 0:
            exploded = "\u2713"
        else:
            exploded = ""

        return state.bindProperties(replace(state.properties, columnsSelector=getColumnsToHighlight2(
            list(map(lambda x: x.expression, state.properties.fsExpressions)), state),
                                            fsExpressions=state.properties.fsExpressions + [FlattenSchemaExpression(
                                                state.properties.delimiter.join(targetColTokens),
                                                SColumn.getSColumn(sanitizedColumn(column)), column, exploded,
                                                targetColTokens)]
                                            ))

    def resultForEachColFunc(self, colInSchema: List[str], state: Component[FlattenSchemaProperties]):

        resultForEachCol = []
        for col in colInSchema:
            tokensOfUsedTargetNames = list(map(lambda x: x.targetTokens, state.properties.fsExpressions))
            targetColTokens = getTargetTokens(col, tokensOfUsedTargetNames)
            colsToExplode = self.getColumnsToExplode([], list(col.split('.')),
                                                     schemaOption=list(map(lambda x: x.schema, state.ports.inputs))[0])
            if len(colsToExplode) != 0:
                exploded = "\u2713"
            else:
                exploded = ""

            resultForEachCol.append((FlattenSchemaExpression(
                state.properties.delimiter.join(targetColTokens),
                SColumn.getSColumn(sanitizedColumn(col)),
                col,
                exploded,
                targetColTokens
            ), colsToExplode))

        return resultForEachCol

    def allColumnsSelectionFunc(self, portId: str, state: Component[FlattenSchemaProperties]):
        columnsInSchema = getColumnsInSchema(portId, state, SchemaFields.LeafLevel)
        resultForEachCol = self.resultForEachColFunc(columnsInSchema, state)

        updatedExpressions = [result[0] for result in resultForEachCol]

        tempExplodeTheseCols = [result[1] for result in resultForEachCol]
        explodeTheseCols = []
        for colList in tempExplodeTheseCols:
            explodeTheseCols.extend(colList)

        return state.bindProperties(replace(state.properties,
                                            fsExpressions=state.properties.fsExpressions + updatedExpressions))

    def dialog(self) -> Dialog:
        return Dialog("FlattenSchema").addElement(
            ColumnsLayout(gap="1rem", height="100%")
                .addColumn(
                PortSchemaTabs(
                    selectedFieldsProperty="columnsSelector",
                    singleColumnClickCallback=self.onClickFunc,
                    allColumnsSelectionCallback=self.allColumnsSelectionFunc
                ).importSchema(), "2fr")
                .addColumn(
                StackLayout(height="100%")
                    .addElement(
                    ColumnsLayout()
                        .addColumn(
                        SelectBox("Columns Delimiter")
                            .addOption("Dash (-)", "-")
                            .addOption("Underscore (_)", "_")
                            .bindProperty("delimiter")
                    )
                )
                    .addElement(
                    BasicTable(
                        "Test",
                        columns=[
                            Column("Target Column", "target", TextBox("", ignoreTitle=True), width="30%"),
                            Column(
                                "Expression",
                                "expression.expression",
                                ExpressionBox(ignoreTitle=True)
                                    .disabled()
                                    .bindPlaceholders()
                                    .bindLanguage("${record.expression.format}")
                                    .withSchemaSuggestions()

                            ),
                            Column("Exploded", "exploded", width="15%")
                        ],
                        appendNewRow=False,
                        height="400px",
                        targetColumnKey="target",
                    ).bindProperty("fsExpressions")
                ),
                "5fr"
            )
        )

    def validate(self, context: WorkflowContext, component: Component[FlattenSchemaProperties]) -> List[Diagnostic]:
        diagnostics = []

        if len(component.properties.fsExpressions) == 0:
            diagnostics.append(Diagnostic("properties.fsExpressions", "At least one expression has to be specified", SeverityLevelEnum.Error))
        else:
            for idx, expr in enumerate(component.properties.fsExpressions):
                if len(expr.target.strip()) == 0:
                    diagnostics.append(Diagnostic(f"properties.fsExpressions[{idx}].target", f"Target cannot be empty.",SeverityLevelEnum.Error))

        return diagnostics

    def onChange(self, context: WorkflowContext, oldState: Component[FlattenSchemaProperties],
                 newState: Component[FlattenSchemaProperties]) -> Component[FlattenSchemaProperties]:

        newProps = newState.properties
        usedCols = getColumnsToHighlight2(list(map(lambda x: x.expression, newProps.fsExpressions)), newState)

        from collections import OrderedDict

        def getCorrectColNameForExploding(ipCol: str, colNameTransformationsDuringExplode: OrderedDict) -> str:
            columnNameCleanOfBackticks = ipCol.replace("`", "")

            if "." not in columnNameCleanOfBackticks:
                return columnNameCleanOfBackticks
            else:
                split_list = columnNameCleanOfBackticks.split('.')
                parentColumn = '.'.join(split_list[:-1])
                lastColumn = split_list[-1]

            if parentColumn in colNameTransformationsDuringExplode.keys():
                return colNameTransformationsDuringExplode.get(parentColumn)[1] + "." + lastColumn
            else:
                return getCorrectColNameForExploding(parentColumn, colNameTransformationsDuringExplode) + "." + lastColumn

        from collections import OrderedDict
        columnNameTransformationMap = OrderedDict()
        explodeColumnsList = []
        fsExpressionList = [fsExp.expression.rawExpression.replace("col(\"", "").replace("\")", "").replace("`", "") for fsExp in newProps.fsExpressions]
        for colmn in fsExpressionList:
            finalColmn = self.getColumnsToExplode([], list(colmn.split('.')), schemaOption=list(map(lambda x: x.schema, newState.ports.inputs))[0])
            explodeColumnsList.extend(finalColmn)
        explodeColumnsList.sort(key=lambda x: len(x.split('.')))

        for column in explodeColumnsList:
            newColNameAfterExplode = column.replace(".", newProps.delimiter)
            correctColNameForExploding = getCorrectColNameForExploding(column, columnNameTransformationMap)
            columnNameTransformationMap[column] = (correctColNameForExploding, newColNameAfterExplode)

        updatedExpressions: List[FlattenSchemaExpression] = newProps.fsExpressions

        updatedExpressionsTmpList:List[FlattenSchemaExpression] = []
        if newProps.delimiter != oldState.properties.delimiter:
            for updatedExpressionsElement in updatedExpressions:
                updatedExpressionsElement = replace(updatedExpressionsElement,
                                                    target=newProps.delimiter.join(
                                                        updatedExpressionsElement.targetTokens),
                                                    flattenedExpression=getCorrectColNameForExploding(
                                                        updatedExpressionsElement.expression.rawExpression.replace("col(\"","").replace("\")",""),
                                                        columnNameTransformationMap))
                updatedExpressionsTmpList.append(updatedExpressionsElement)

        else:
            for updatedExpressionsElement in updatedExpressions:
                updatedExpressionsElement = replace(updatedExpressionsElement,
                                                    flattenedExpression=getCorrectColNameForExploding(
                                                        updatedExpressionsElement.expression.rawExpression.replace("col(\"","").replace("\")",""),
                                                        columnNameTransformationMap))
                updatedExpressionsTmpList.append(updatedExpressionsElement)

        explodeColInfoList = []

        for key in columnNameTransformationMap.keys():
            explodeColInfoList.append(
                ExplodeColInfo(key, columnNameTransformationMap[key][0], columnNameTransformationMap[key][1]))

        return newState.bindProperties(replace(newProps, columnsSelector=usedCols,
                                               fsExpressions=updatedExpressionsTmpList,
                                               explodedColsNewName=explodeColInfoList))

    class FlattenSchemaCode(ComponentCode):
        def __init__(self, newProps):
            self.props: FlattenSchema.FlattenSchemaProperties = newProps

        def apply(self, spark: SparkSession, in0: DataFrame) -> DataFrame:
            flattened = in0

            for explodedColsNewNames in self.props.explodedColsNewName:

                flattened = flattened.withColumn(explodedColsNewNames.colNameAfterExplode,
                                                 explode_outer(explodedColsNewNames.updatedColToExplode))

            flt_col: SubstituteDisabled = flattened.columns
            selectCols: SubstituteDisabled = [
                col((_exp.target)) if _exp.target in flt_col else col(_exp.flattenedExpression).alias(_exp.target)
                for _exp in self.props.fsExpressions
            ]

            return flattened.select(*selectCols)
