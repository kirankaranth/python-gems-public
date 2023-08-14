from prophecy.cb.server.base.datatypes import SInt

from prophecy.cb.server.base.ComponentBuilderBase import *
from pyspark.sql import *
from pyspark.sql.functions import *

from prophecy.cb.ui.UISpecUtil import ColumnsUsage, validateSColumn, getColumnsToHighlight2
from prophecy.cb.ui.uispec import *
from prophecy.cb.server.base import WorkflowContext

@dataclass(frozen=True)
class RangeRepartitionExpression:
    expression: SColumn
    sortType: str


class Repartition(ComponentSpec):
    name: str = "Repartition"
    category: str = "Join/Split"
    gemDescription: str = "Splits a Dataframe across workers using Repartition or Coalesce."
    docUrl: str = "https://docs.prophecy.io/low-code-spark/gems/join-split/Repartition"

    def optimizeCode(self) -> bool:
        return True

    @dataclass(frozen=True)
    class RepartitionProperties(ComponentProperties):
        columnsSelector: List[str] = field(default_factory=list)
        repartitionType: str = "coalesce"
        nPartitions: Optional[SInt] = SInt("10")
        defaultPartitions: Optional[SInt] = None
        overwriteDefaultNPartitions: bool = False
        hashExpressions: List[SColumn] = field(default_factory=list)
        rangeExpressions: List[RangeRepartitionExpression] = field(default_factory=list)

    def dialog(self) -> Dialog:
        selectBox = (RadioGroup("")
                     .addOption("Coalesce", "coalesce",
                                description=("Reduces the number of partitions without shuffling the dataset."))
                     .addOption("Random Repartitioning", "random_repartitioning",
                                description="Repartitions without data distribution defined. Reshuffles the dataset."
                                )
                     .addOption("Hash Repartitioning", "hash_repartitioning",
                                description="Repartitions, spreading the data evenly across various partitions based on the key. Reshuffles the dataset."
                                )
                     .addOption("Range Repartitioning", "range_repartitioning",
                                description="Repartitions, spreading the data with tuples having keys within the same range on the the same worker. Reshuffles the dataset."
                                )
                     .setOptionType("button")
                     .setVariant("medium")
                     .setButtonStyle("solid")
                     .bindProperty("repartitionType")
                     )

        sortSelectBox = (SelectBox("")
                         .addOption("Ascending", "asc")
                         .addOption("Descending", "desc"))

        numberOfPartitions = (ExpressionBox("Number of Partitions")
                              .bindPlaceholder("10")
                              .bindProperty("nPartitions")
                              .withFrontEndLanguage())

        defaultPartitions = (ExpressionBox("Number of Partitions")
                             .bindPlaceholder("spark.sql.shuffle.partitions")
                             .bindProperty("defaultPartitions")
                             .withFrontEndLanguage())

        coalesceView = (Condition()
                        .ifEqual(PropExpr("component.properties.repartitionType"), StringExpr("coalesce"))
                        .then(numberOfPartitions))

        randomRepartitionView = (Condition()
                                 .ifEqual(PropExpr("component.properties.repartitionType"),
                                          StringExpr("random_repartitioning"))
                                 .then(numberOfPartitions))

        hashRepartitionView = (Condition()
            .ifEqual(PropExpr("component.properties.repartitionType"),
                     StringExpr("hash_repartitioning"))
            .then(
            StackLayout()
                .addElement(ColumnsLayout()
                            .addColumn(Checkbox("Overwrite default number of partitions")
                                       .bindProperty("overwriteDefaultNPartitions"))
                            .addColumn(Condition()
                                       .ifEqual(PropExpr("component.properties.overwriteDefaultNPartitions"),
                                                BooleanExpr(True))
                                       .then(defaultPartitions)
                                       .otherwise(defaultPartitions.disabled())))
                .addElement(BasicTable("Test",
                                       height=("400px"),
                                       columns=[
                                           Column("Repartition Expression", "expression",
                                                  (ExpressionBox(ignoreTitle=True)
                                                   .bindLanguage("${record.format}")
                                                   .bindPlaceholders()
                                                   .withSchemaSuggestions()))])
                            .bindProperty("hashExpressions"))))

        rangeRepartitionView = (Condition()
            .ifEqual(PropExpr("component.properties.repartitionType"),
                     StringExpr("range_repartitioning"))
            .then(
            StackLayout()
                .addElement(
                ColumnsLayout()
                    .addColumn(Checkbox("Overwrite default number of partitions")
                               .bindProperty("overwriteDefaultNPartitions"))
                    .addColumn(Condition()
                               .ifEqual(PropExpr("component.properties.overwriteDefaultNPartitions"), BooleanExpr(True))
                               .then(defaultPartitions)
                               .otherwise(defaultPartitions.disabled())))
                .addElement(BasicTable("Test", height=("400px"),
                                       columns=[
                                           Column("Repartition Expression", "expression.expression", (
                                               ExpressionBox(ignoreTitle=True)
                                                   .bindLanguage(
                                                   "${record.expression.format}")
                                                   .bindPlaceholders()
                                                   .withSchemaSuggestions())),
                                           Column("Sort", "sortType", (sortSelectBox), width="25%")])
                            .bindProperty("rangeExpressions"))))

        return Dialog("Repartition").addElement(
            ColumnsLayout(gap=("1rem"), height=("100%"))
                .addColumn(PortSchemaTabs().importSchema())
                .addColumn(
                StackLayout()
                    .addElement(selectBox)
                    .addElement(coalesceView)
                    .addElement(randomRepartitionView)
                    .addElement(hashRepartitionView)
                    .addElement(rangeRepartitionView),
                "2fr"
            )
        )

    def validate(self, context: WorkflowContext, component: Component[RepartitionProperties]) -> List[Diagnostic]:
        diagnostics = []

        if (
                component.properties.repartitionType == "hash_repartitioning" or component.properties.repartitionType == "range_repartitioning"):
            if (component.properties.overwriteDefaultNPartitions):
                if component.properties.defaultPartitions is not None:
                    for message in component.properties.defaultPartitions.diagnosticMessages:
                        diagnostics.append(Diagnostic(
                            "properties.defaultPartitions",
                            message,
                            SeverityLevelEnum.Error
                        ))

            if (component.properties.repartitionType == "hash_repartitioning"):
                if (len(component.properties.hashExpressions) == 0):
                    diagnostics.append(Diagnostic(
                        "properties.hashExpressions",
                        "At least one expression has to be specified",
                        SeverityLevelEnum.Error
                    ))
                else:
                    for idx, expr in enumerate(component.properties.hashExpressions):
                        diagnostics.extend(validateSColumn(
                            expr,
                            "hashExpressions[{idx}]",
                            component,
                            testColumnPresence=ColumnsUsage.WithoutInputAlias
                        ))
            else:
                if (len(component.properties.rangeExpressions) == 0):
                    diagnostics.append(Diagnostic("properties.rangeExpressions",
                                                  "At least one expression has to be specified",
                                                  SeverityLevelEnum.Error
                                                  ))
                else:
                    for idx, expr in enumerate(component.properties.rangeExpressions):
                        diagnostics.extend(validateSColumn(
                            expr.expression,
                            f"rangeExpressions[{idx}].expression",
                            component,
                            (ColumnsUsage.WithoutInputAlias)
                        ))
        elif component.properties.nPartitions.diagnosticMessages is not None:
            for message in component.properties.nPartitions.diagnosticMessages:
                diagnostics.append(Diagnostic("properties.nPartitions", message, SeverityLevelEnum.Error))

        return diagnostics

    def onChange(self, context: WorkflowContext, oldState: Component[RepartitionProperties], newState: Component[RepartitionProperties]) -> \
            Component[
                RepartitionProperties]:

        newProps = newState.properties
        usedCols = getColumnsToHighlight2(newProps.hashExpressions + [x.expression for x in newProps.rangeExpressions],
                                          newState)
        return newState.bindProperties(replace(newProps, columnsSelector=usedCols))

    class RepartitionCode(ComponentCode):
        def __init__(self, newProps):
            self.props: Repartition.RepartitionProperties = newProps

        def apply(self, spark: SparkSession, in0: DataFrame) -> DataFrame:
            numberOfPartitions = self.props.nPartitions.value

            if self.props.repartitionType == "coalesce":
                out = in0.coalesce(numberOfPartitions)
            elif self.props.repartitionType == "random_repartitioning":
                out = in0.repartition(numberOfPartitions)
            elif self.props.repartitionType == "hash_repartitioning":
                hashExpColumns = map(lambda x: x.column(), self.props.hashExpressions)
                if self.props.overwriteDefaultNPartitions:
                    out = in0.repartition(int(self.props.defaultPartitions.value), *hashExpColumns)
                else:
                    out = in0.repartition(*hashExpColumns)

            else:
                rangeExpressionRules = map(
                    lambda x: x.expression.column().asc() if (x.sortType == "asc") else x.expression.column().desc(),
                    self.props.rangeExpressions)
                if (self.props.overwriteDefaultNPartitions):
                    out = in0.repartitionByRange(int(self.props.defaultPartitions.value), *rangeExpressionRules)
                else:
                    out = in0.repartitionByRange(*rangeExpressionRules)

            return out
