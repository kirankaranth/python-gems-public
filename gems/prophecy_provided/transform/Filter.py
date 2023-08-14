from prophecy.cb.server.base.ComponentBuilderBase import *
from pyspark.sql import *
from pyspark.sql.functions import *

from prophecy.cb.ui.UISpecUtil import getColumnsToHighlight2, validateSColumn
from prophecy.cb.ui.uispec import *
from prophecy.cb.util.StringUtils import isBlank
from prophecy.cb.server.base import WorkflowContext

class Filter(ComponentSpec):
    name: str = "Filter"
    category: str = "Transform"
    gemDescription: str = "Filters rows of the DataFrame based on the provided filter conditions."
    docUrl: str = "https://docs.prophecy.io/low-code-spark/gems/transform/filter/"


    def optimizeCode(self) -> bool:
        return True

    @dataclass(frozen=True)
    class FilterProperties(ComponentProperties):
        columnsSelector: List[str] = field(default_factory=list)
        condition: SColumn = SColumn("lit(True)")

    def dialog(self) -> Dialog:
        return Dialog("Filter").addElement(
            ColumnsLayout(height="100%")
                .addColumn(PortSchemaTabs(selectedFieldsProperty=("columnsSelector")).importSchema(), "2fr")
                .addColumn(StackLayout(height=("100%"))
                .addElement(TitleElement("Filter Condition"))
                .addElement(
                Editor(height=("100%")).withSchemaSuggestions().bindProperty("condition.expression")
            ), "5fr"))

    def validate(self, context: WorkflowContext, component: Component[FilterProperties]) -> List[Diagnostic]:
        return validateSColumn(component.properties.condition, "condition", component)

    def onChange(self, context: WorkflowContext, oldState: Component[FilterProperties], newState: Component[FilterProperties]) -> Component[
        FilterProperties]:
        newProps = newState.properties
        usedColExps = getColumnsToHighlight2([newProps.condition], newState)
        return newState.bindProperties(replace(newProps, columnsSelector=usedColExps))

    class FilterCode(ComponentCode):
        def __init__(self, newProps):
            self.props: Filter.FilterProperties = newProps

        def apply(self, spark: SparkSession, in0: DataFrame) -> DataFrame:
            return in0.filter(self.props.condition.column())
