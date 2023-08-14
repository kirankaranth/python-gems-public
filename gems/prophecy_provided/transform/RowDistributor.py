from pyspark.sql import *

from prophecy.cb.server.base.ComponentBuilderBase import *
from prophecy.cb.ui.uispec import *
from prophecy.cb.ui.UISpecUtil import validateSColumn
from prophecy.cb.util.StringUtils import isBlank
from prophecy.cb.server.base import WorkflowContext

@dataclass(frozen=True)
class FileTab:
    path: str = "out0"
    id: str = "out0"
    model: SColumn = SColumn("lit(True)")


class RowDistributor(ComponentSpec):
    name: str = "RowDistributor"
    category: str = "Join/Split"
    gemDescription: str = "Splits one Dataframe into multiple Dataframes by rows based on filter conditions."
    docUrl: str = "https://docs.prophecy.io/low-code-spark/gems/join-split/row-distributor"

    def optimizeCode(self) -> bool:
        return True

    @dataclass(frozen=True)
    class RowDistributorProperties(ComponentProperties):
        outports: List[FileTab] = field(
            default_factory=lambda: [FileTab(path="out0", id="out0", model=SColumn("lit(True)")),
                                     FileTab(path="out1", id="out1", model=SColumn("lit(True)"))])
        distributeUnmatchedRows: Optional[bool] = None

    def dialog(self) -> Dialog:
        placeholders = {
            "scala": """col("some_column") > lit(10)""",
            "python": """col("some_column") > lit(10)""",
            "sql": """some_column>10"""}
        return Dialog("RowDistributor").addElement(
            ColumnsLayout(gap="1rem", height="100%")
                .addColumn(PortSchemaTabs(allowOutportRename=True, allowOutportAddDelete=True).importSchema())
                .addColumn(StackLayout(height="100%") \
                    .addElement(
                        Checkbox("Distribute Unmatched Rows").bindProperty(
                                    "distributeUnmatchedRows")
                    ) \
                    .addElement(
                        FileEditor(
                            newFilePrefix="out",
                            newFileLanguage="${$.workflow.metainfo.frontEndLanguage}",
                            minFiles=1
                        )
                        .withSchemaSuggestions()
                        .withExpressionMode()
                        .bindPlaceholders(placeholders)
                        .bindProperty("outports"),
                    ),
                "2fr"
            )
        )

    def validate(self, context: WorkflowContext, component: Component[RowDistributorProperties]) -> List[Diagnostic]:
        diagnostics = []
        min_files = 2 if not component.properties.distributeUnmatchedRows else 1
        if len(component.ports.outputs) < min_files:
            diagnostics.append(Diagnostic(
                "properties.outports",
                "Number of output ports cannot be less than two.",
                SeverityLevelEnum.Error
            ))
        if not component.properties.distributeUnmatchedRows and len(component.ports.outputs) != len(component.properties.outports):
            diagnostics.append(Diagnostic(
                "properties.outports",
                "Number of output ports have to be the same as the number of file tabs.",
                SeverityLevelEnum.Error
            ))

        for idx1, outport in enumerate(component.properties.outports):
            if isBlank(outport.model.rawExpression):
                diagnostics.append(Diagnostic(
                    f"properties.outports[{idx1}]",
                    f"{outport.path} is empty",
                    SeverityLevelEnum.Error
                ))

        return diagnostics

    def onChange(self, context: WorkflowContext, oldState: Component[RowDistributorProperties], newState: Component[RowDistributorProperties]) -> \
            Component[RowDistributorProperties]:
        currentTabs = newState.properties.outports
        revisedFileTabs = []
        newOutputs = [op for op in newState.ports.outputs if (
                    op.id != newState.id + "_orElse" or op.slug == "orElse")]  # during copy, all port ids are changed, so cannot rely on port ids


        for port in newOutputs:
            if port.slug == "orElse":
                continue
            matchingTab = [tab for tab in currentTabs if (tab.id == port.id)]
            newFileTab = FileTab(port.slug, port.id, SColumn(""))
            if len(matchingTab) != 0:
                newFileTab = replace(newFileTab, model=matchingTab[0].model)
            revisedFileTabs.append(newFileTab)


        doesOrElsePortExist = [op for op in newState.ports.outputs if op.slug == "orElse"]
        if newState.properties.distributeUnmatchedRows:
            if len(doesOrElsePortExist) == 0:
                newOutputs.append(NodePort(newState.id + "_orElse", "orElse", newOutputs[0].schema))
        elif not newState.properties.distributeUnmatchedRows:
            if len(doesOrElsePortExist) >= 1:
                newOutputs = [op for op in newOutputs if op.slug != "orElse"]

        updatedPorts = replace(newState.ports, outputs=newOutputs)
        updatedProperties = replace(newState.properties, outports=revisedFileTabs)
        return replace(newState, properties=updatedProperties, ports=updatedPorts)

    class RowDistributorCode(ComponentCode):
        def __init__(self, newProps):
            self.props: RowDistributor.RowDistributorProperties = newProps

        def apply(self, spark: SparkSession, in0: DataFrame) -> (DataFrame, DataFrame, List[DataFrame]):
            list_df = []
            conditions = []
            for outport in self.props.outports:
                list_df.append(in0.filter(outport.model.column()))
                conditions.append(outport.model.column())

            if self.props.distributeUnmatchedRows:
                elseCond = ~ conditions[0]
                if len(conditions) > 1:
                    for c in conditions[1:]:
                        elseCond = elseCond & (~ c)
                list_df.append(in0.filter(elseCond))

            return list_df[0], list_df[1], list_df[2:]